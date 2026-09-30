"""LLM-based zero-shot email/document classifier with structured outputs."""

from __future__ import annotations

import logging
import os
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema (strict structured output)
# ---------------------------------------------------------------------------

ROUTING_CATEGORIES = (
    "Standards & Regulations",
    "Certification",
    "Testing & Laboratory",
    "Product & Technical Support",
    "General / Other",
)


class DepartmentCategory(str, Enum):
    STANDARDS_REGULATIONS = "Standards & Regulations"
    CERTIFICATION = "Certification"
    TESTING_LABORATORY = "Testing & Laboratory"
    PRODUCT_TECH_SUPPORT = "Product & Technical Support"
    GENERAL_OTHER = "General / Other"


class ConfidenceScore(BaseModel):
    category: DepartmentCategory
    score: float = Field(ge=0.0, le=1.0, description="Confidence between 0.0 and 1.0")


class CategoryAttachmentMap(BaseModel):
    category: DepartmentCategory
    filenames: List[str] = Field(
        default_factory=list,
        description="Attachment filenames assigned to this department (must match provided filenames exactly)",
    )


class RoutedDepartment(BaseModel):
    category: DepartmentCategory
    confidence: float = Field(ge=0.0, le=1.0)
    attachment_filenames: List[str] = Field(default_factory=list)
    executive_summary: str = Field(
        description="2-3 sentence technical summary tailored for this department reviewer"
    )
    reasoning: str = Field(description="Brief explanation of why this category and attachments were selected")


class PrimaryIntent(str, Enum):
    CERTIFICATION = "Certification"
    COMPLIANCE_REGULATORY = "Compliance / Regulatory"
    LAB_TESTING = "Lab Testing"
    TECHNICAL_SUPPORT = "Technical Support"
    COURSE_ASSIGNMENT = "Course Assignment"
    GENERAL_INQUIRY = "General Inquiry"


class IntentAnalysis(BaseModel):
    primary_intent: PrimaryIntent = Field(
        description="Core goal the sender is trying to accomplish with this email/documents"
    )
    intent_confidence: float = Field(ge=0.0, le=1.0)
    intent_rationale: str = Field(
        description="1-2 sentences explaining the inferred primary purpose or expected action"
    )
    secondary_intents: List[str] = Field(
        default_factory=list,
        description="Auxiliary intent tags (e.g. Audit Readiness, SLA Follow-up)",
    )


class LLMClassificationOutput(BaseModel):
    """Structured triage result consumed by pipeline.resolve_routing()."""

    winning_categories: List[DepartmentCategory] = Field(
        description="Qualifying departments; may include multiple entries for multi-route cases"
    )
    confidence_scores: List[ConfidenceScore] = Field(
        description="Confidence score for every routing category evaluated"
    )
    category_attachments: List[CategoryAttachmentMap] = Field(
        description="Maps each qualified department to its relevant attachment filename(s)"
    )
    routes: List[RoutedDepartment] = Field(
        description="Per-department routing details including summary and reasoning"
    )
    reasoning: str = Field(description="Overall triage reasoning across all routes")
    intent_analysis: IntentAnalysis = Field(
        description="Sender/document intent independent of department routing"
    )
    client_name: str = Field(
        description=(
            "Client, vendor, manufacturer, or organization requesting service or sending documents "
            "(e.g. Apple, Samsung, Amphenol, General Electric). Infer from sender, signature, "
            "document headers, or certificates. Do not use 'Unknown'; prefer sender organization or domain."
        )
    )

    @field_validator("winning_categories")
    @classmethod
    def require_winning_categories(cls, value):
        if not value:
            raise ValueError("At least one winning category is required")
        return value


def get_classification_json_schema() -> dict:
    """Return the JSON Schema used for OpenAI structured output / tool calling."""
    return LLMClassificationOutput.model_json_schema()


# ---------------------------------------------------------------------------
# Prompt & context assembly
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an enterprise email triage classifier for a testing and regulatory organization.

Classify each attachment as its own document object using extracted body text — NOT filenames.
Filenames can be misleading (e.g. "new pdf testing.pdf" may contain ISO 9001 standards content).

Departments:
1. Standards & Regulations — ISO/IEC standards, regulatory frameworks, compliance clauses.
2. Certification — UL certification, listings, marks, factory audits, conformity certificates.
3. Testing & Laboratory — clinical/lab test reports, sample results, assays, test plans.
4. Product & Technical Support — product specs, troubleshooting, datasheets, operational guides.
5. General / Other — ONLY when no specialized category applies.

Rules:
- Classify EACH attachment independently from its document excerpt.
- Each attachment filename must appear in AT MOST ONE category_attachments entry (exclusive assignment).
- winning_categories must ONLY include departments that received at least one attachment OR clearly own the email body.
- Do NOT include General / Other or Product & Technical Support if Standards or Testing categories already have attachments.
- Multi-route only when distinct documents belong to distinct departments (e.g. ISO standard PDF + pathology lab report).
- Use exact attachment filenames from the provided list; never invent names.
- Ground every assignment in document body content, not filename keywords.
- Assign confidence_scores for all five categories (0.0–1.0).

Also infer intent_analysis for the overall email:
- primary_intent: one of Certification, Compliance / Regulatory, Lab Testing, Technical Support, Course Assignment, General Inquiry
- intent_confidence: 0.0–1.0
- intent_rationale: what the sender is trying to accomplish (1–2 sentences)
- secondary_intents: optional short tags for auxiliary goals

Identify client_name: the company or organization the submission is for or from (not the mailbox provider).
Use document content and sender when possible; avoid returning Unknown.
"""

LLM_CONTEXT_MAX_CHARS = int(os.environ.get("LLM_CONTEXT_MAX_CHARS", "28000"))
LLM_PER_DOC_HEAD_CHARS = int(os.environ.get("LLM_PER_DOC_HEAD_CHARS", "1800"))
LLM_PER_DOC_TAIL_CHARS = int(os.environ.get("LLM_PER_DOC_TAIL_CHARS", "600"))
LLM_EMAIL_MAX_CHARS = int(os.environ.get("LLM_EMAIL_MAX_CHARS", "4000"))
LLM_TIMEOUT_SECONDS = float(os.environ.get("LLM_TIMEOUT_SECONDS", "45"))
MIN_LLM_CONFIDENCE = float(os.environ.get("MIN_LLM_CONFIDENCE", "0.45"))


def _truncate(text: str, limit: int) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


def build_document_snippets(document_parts: List[Dict[str, str]], max_chars: int = LLM_CONTEXT_MAX_CHARS) -> List[str]:
    """Build bounded text snippets from extracted document parts (handles large PDFs)."""
    snippets = []
    used = 0

    for part in document_parts or []:
        filename = part.get("filename") or "attachment"
        text = (part.get("text") or "").strip()
        if not text:
            continue

        if len(text) <= LLM_PER_DOC_HEAD_CHARS + LLM_PER_DOC_TAIL_CHARS:
            snippet = f"[{filename}]\n{text}"
        else:
            head = text[:LLM_PER_DOC_HEAD_CHARS]
            tail = text[-LLM_PER_DOC_TAIL_CHARS:]
            snippet = (
                f"[{filename}]\n"
                f"{head}\n\n...[middle truncated]...\n\n{tail}"
            )

        if used + len(snippet) > max_chars:
            remaining = max_chars - used
            if remaining < 200:
                break
            snippet = _truncate(snippet, remaining)

        snippets.append(snippet)
        used += len(snippet)
        if used >= max_chars:
            break

    return snippets


def build_llm_user_prompt(
    subject: str,
    body: str,
    snippet: str,
    attachment_names: Optional[List[str]] = None,
    document_parts: Optional[List[Dict[str, str]]] = None,
) -> str:
    email_block = _truncate(
        f"Subject: {subject or '(none)'}\n\n{(body or snippet or '').strip()}",
        LLM_EMAIL_MAX_CHARS,
    )
    names = attachment_names or []
    per_doc_sections = []
    parts_by_name = {}
    for part in document_parts or []:
        fname = part.get("filename") or ""
        parts_by_name[fname] = part
        parts_by_name[os.path.basename(fname)] = part

    for index, name in enumerate(names, start=1):
        part = parts_by_name.get(name) or parts_by_name.get(os.path.basename(name)) or {}
        workspace_path = part.get("workspace_path") or "(not in workspace)"
        text = (part.get("text") or "").strip()
        if text:
            excerpt = text[: LLM_PER_DOC_HEAD_CHARS] + ("..." if len(text) > LLM_PER_DOC_HEAD_CHARS else "")
        else:
            excerpt = "(no preview — file stored in workspace; classify from email body and filename as needed)"
        per_doc_sections.append(
            f"### Document {index}: {name}\n"
            f"Workspace path: {workspace_path}\n"
            f"Filename (may be misleading): {name}\n"
            f"Triage preview only (agents read full file from workspace on demand):\n{excerpt}"
        )

    if not per_doc_sections:
        doc_snippets = build_document_snippets(document_parts or [])
        docs_block = "\n\n---\n\n".join(doc_snippets) if doc_snippets else "(no attachments)"
    else:
        docs_block = "\n\n---\n\n".join(per_doc_sections)

    return (
        f"## Email\n{email_block}\n\n"
        f"## Attachments ({len(names)})\n"
        + "\n".join(f"- {name}" for name in names)
        + f"\n\n## Per-Document Classification Input\n{docs_block}"
    )


# ---------------------------------------------------------------------------
# OpenAI client (Azure or standard)
# ---------------------------------------------------------------------------

def get_llm_client():
    azure_key = os.environ.get("AZURE_OPENAI_API_KEY", "").strip()
    azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").strip()
    if azure_key and azure_endpoint:
        from openai import AzureOpenAI

        return AzureOpenAI(
            api_key=azure_key,
            azure_endpoint=azure_endpoint,
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            timeout=LLM_TIMEOUT_SECONDS,
        ), os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "No LLM credentials: set AZURE_OPENAI_API_KEY + AZURE_OPENAI_ENDPOINT or OPENAI_API_KEY"
        )
    from openai import OpenAI

    return OpenAI(api_key=api_key, timeout=LLM_TIMEOUT_SECONDS), os.environ.get(
        "OPENAI_MODEL", "gpt-4o-mini"
    )


def classify_via_llm(
    subject: str,
    body: str,
    snippet: str,
    attachment_names: Optional[List[str]] = None,
    document_parts: Optional[List[Dict[str, str]]] = None,
) -> LLMClassificationOutput:
    """Call the LLM with structured output parsing. Raises on API/validation errors."""
    client, model = get_llm_client()
    user_content = build_llm_user_prompt(
        subject,
        body,
        snippet,
        attachment_names=attachment_names,
        document_parts=document_parts,
    )

    completion = client.beta.chat.completions.parse(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        response_format=LLMClassificationOutput,
        temperature=float(os.environ.get("LLM_TEMPERATURE", "0.1")),
    )
    parsed = completion.choices[0].message.parsed
    if parsed is None:
        raise RuntimeError("LLM returned empty parsed response")
    return parsed


# ---------------------------------------------------------------------------
# Adapter: LLM output -> pipeline analysis contract
# ---------------------------------------------------------------------------

def _rule_for_category(rules: List[dict], category: str, fallback: dict) -> dict:
    for rule in rules:
        if (rule.get("category") or rule.get("department")) == category:
            return rule
    if category == "General / Other" and fallback:
        return fallback
    return {}


SPECIALIZED_CATEGORIES = (
    "Standards & Regulations",
    "Certification",
    "Testing & Laboratory",
    "Product & Technical Support",
)


def _isolate_attachment_map(
    raw_map: Dict[str, List[str]],
    attachment_names: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Each attachment may appear in exactly one department bucket."""
    allowed = attachment_names or []
    allowed_lookup = {name.lower(): name for name in allowed}
    claimed = {}
    order = list(SPECIALIZED_CATEGORIES) + ["General / Other"]

    for category in order:
        for filename in raw_map.get(category) or []:
            key = (filename or "").strip().lower()
            canonical = allowed_lookup.get(key) or filename
            if not canonical or canonical in claimed:
                continue
            claimed[canonical] = category

    isolated = {category: [] for category in order}
    for filename, category in claimed.items():
        isolated.setdefault(category, []).append(filename)
    return {cat: files for cat, files in isolated.items() if files}


def _normalize_attachment_names(names: List[str], allowed: Optional[List[str]]) -> List[str]:
    if not names:
        return []
    allowed_set = {n.lower(): n for n in (allowed or [])}
    normalized = []
    for name in names:
        key = (name or "").strip().lower()
        if key in allowed_set:
            normalized.append(allowed_set[key])
        elif name in (allowed or []):
            normalized.append(name)
    return normalized


def llm_output_to_routing_result(
    llm_result: LLMClassificationOutput,
    rules: List[dict],
    fallback: dict,
    attachment_names: Optional[List[str]] = None,
    use_fallback: bool = True,
) -> Tuple[Optional[dict], float, dict, List[dict]]:
    """
    Convert LLM structured output into the same tuple as classify_from_signals():
    (primary_rule, score, analysis, winning_rules)
    """
    attachment_names = attachment_names or []

    confidence_map = {item.category.value: item.score for item in llm_result.confidence_scores}
    raw_attachment_map: Dict[str, List[str]] = {}
    for item in llm_result.category_attachments:
        cat = item.category.value
        raw_attachment_map[cat] = _normalize_attachment_names(item.filenames, attachment_names)

    department_summaries: Dict[str, str] = {}
    department_reasoning: Dict[str, str] = {}
    for route in llm_result.routes:
        cat = route.category.value
        department_summaries[cat] = route.executive_summary.strip()
        department_reasoning[cat] = route.reasoning.strip()
        if route.attachment_filenames:
            merged = _normalize_attachment_names(route.attachment_filenames, attachment_names)
            if merged:
                raw_attachment_map[cat] = merged

    attachment_map = _isolate_attachment_map(raw_attachment_map, attachment_names)
    has_specialized_files = any(
        attachment_map.get(cat)
        for cat in ("Standards & Regulations", "Certification", "Testing & Laboratory")
    )

    qualifying = []
    for cat_enum in llm_result.winning_categories:
        cat = cat_enum.value
        score = confidence_map.get(cat, 0.0)
        if score < MIN_LLM_CONFIDENCE:
            continue
        if has_specialized_files and cat in ("General / Other", "Product & Technical Support"):
            if not attachment_map.get(cat):
                continue
        if attachment_map and not attachment_map.get(cat):
            continue
        qualifying.append(cat)

    if attachment_map and not qualifying:
        qualifying = [cat for cat in SPECIALIZED_CATEGORIES if attachment_map.get(cat)]

    qualifying = list(dict.fromkeys(qualifying))

    if not qualifying:
        if use_fallback and fallback:
            fb_cat = fallback.get("category") or "General / Other"
            analysis = {
                "method": "llm_fallback",
                "llm_error": None,
                "llm_reasoning": llm_result.reasoning,
                "winning_category": fb_cat,
                "winning_score": 0.0,
                "qualifying_categories": [fb_cat],
                "winning_categories": [fb_cat],
                "category_scores": confidence_map,
                "category_attachments": {},
                "department_summaries": {},
                "department_reasoning": {},
                "winning_rules": [fallback],
                "multi_route": False,
            }
            return fallback, 0.0, analysis, [fallback]
        return None, 0.0, {"method": "llm_none", "category_scores": confidence_map}, []

    winning_rules = []
    category_details = []
    for cat in qualifying:
        rule = _rule_for_category(rules, cat, fallback)
        if not rule:
            continue
        score = confidence_map.get(cat, 0.0)
        dept_attachments = attachment_map.get(cat, [])
        source = f"llm:attachment:{dept_attachments[0]}" if dept_attachments else "llm:email"
        winning_rules.append(rule)
        category_details.append(
            {
                "category": cat,
                "recipient": rule.get("recipient") or rule.get("target_email"),
                "score": round(score, 4),
                "matched_keywords": [],
                "source": source,
                "chunk_id": None,
                "char_range": None,
                "qualified": True,
                "qualification_reason": "llm_confidence",
                "reasoning": department_reasoning.get(cat, llm_result.reasoning),
                "text_preview": department_summaries.get(cat, "")[:240],
            }
        )

    if not winning_rules:
        if use_fallback and fallback:
            analysis = {
                "method": "llm_fallback",
                "llm_reasoning": llm_result.reasoning,
                "qualifying_categories": [fallback.get("category") or "General / Other"],
                "winning_rules": [fallback],
                "category_scores": confidence_map,
            }
            return fallback, 0.0, analysis, [fallback]
        return None, 0.0, {"method": "llm_none"}, []

    primary_category = qualifying[0]
    primary_score = confidence_map.get(primary_category, 0.0)
    primary_rule = winning_rules[0]

    final_attachments = {cat: attachment_map.get(cat, []) for cat in qualifying}

    analysis = {
        "method": "llm",
        "llm_reasoning": llm_result.reasoning,
        "llm_raw_winning_categories": [c.value for c in llm_result.winning_categories],
        "winning_category": primary_category,
        "winning_score": round(primary_score, 4),
        "winning_source": "llm",
        "matched_keywords": [],
        "chunk_id": None,
        "char_range": None,
        "text_preview": department_summaries.get(primary_category, "")[:240],
        "category_scores": {cat: round(confidence_map.get(cat, 0.0), 4) for cat in ROUTING_CATEGORIES},
        "category_details": category_details,
        "qualifying_threshold": {"min_confidence": MIN_LLM_CONFIDENCE},
        "winning_rules": winning_rules,
        "winning_categories": qualifying,
        "qualifying_categories": qualifying,
        "qualifying_rule_count": len(winning_rules),
        "winning_recipients": [
            r.get("recipient") or r.get("target_email")
            for r in winning_rules
            if r.get("recipient") or r.get("target_email")
        ],
        "category_attachments": final_attachments,
        "department_summaries": department_summaries,
        "department_reasoning": department_reasoning,
        "intent_analysis": llm_result.intent_analysis.model_dump(),
        "client_name": (llm_result.client_name or "").strip(),
        "multi_route": len(qualifying) > 1,
    }
    return primary_rule, primary_score, analysis, winning_rules


def classify_with_llm_or_fallback(
    subject: str,
    body: str,
    snippet: str,
    attachment_names: Optional[List[str]] = None,
    document_parts: Optional[List[Dict[str, str]]] = None,
    rules: Optional[List[dict]] = None,
    fallback: Optional[dict] = None,
    use_fallback: bool = True,
) -> Tuple[Optional[dict], float, dict, List[dict]]:
    """Run LLM classification. Raises on API/parse errors for pipeline keyword fallback."""
    rules = rules if rules is not None else []
    fallback = fallback or {}

    llm_result = classify_via_llm(
        subject,
        body,
        snippet,
        attachment_names=attachment_names,
        document_parts=document_parts,
    )
    return llm_output_to_routing_result(
        llm_result,
        rules,
        fallback,
        attachment_names=attachment_names,
        use_fallback=use_fallback,
    )
