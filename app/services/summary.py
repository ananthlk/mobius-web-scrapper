"""Optional LLM summary of scraped content."""
import logging
import os

logger = logging.getLogger(__name__)

# Max chars to send to LLM
MAX_INPUT_CHARS = 12000


def summarize_content(aggregated_text: str) -> str | None:
    """
    Summarize scraped page content using OpenAI (if OPENAI_API_KEY set) or Vertex.
    Returns summary string or None if LLM not configured or fails.
    """
    if not aggregated_text or not aggregated_text.strip():
        return None

    text = aggregated_text.strip()
    if len(text) > MAX_INPUT_CHARS:
        text = text[:MAX_INPUT_CHARS] + "\n\n[... truncated for summary ...]"

    # Try OpenAI first
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        try:
            return _summarize_openai(text, api_key)
        except Exception as e:
            logger.warning("OpenAI summary failed: %s", e)

    # Try Vertex if configured (same pattern as mobius-rag)
    if os.getenv("VERTEX_PROJECT_ID") and os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            return _summarize_vertex(text)
        except Exception as e:
            logger.warning("Vertex summary failed: %s", e)

    return None


def _summarize_openai(text: str, api_key: str) -> str:
    import httpx

    resp = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            "messages": [
                {
                    "role": "system",
                    "content": "Summarize the following webpage content in 2-4 concise paragraphs. Focus on the main topics, key information, and purpose of the page.",
                },
                {"role": "user", "content": text},
            ],
            "max_tokens": 500,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    choice = data.get("choices", [{}])[0]
    return (choice.get("message", {}).get("content") or "").strip()


def _summarize_vertex(text: str) -> str:
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel

        project = os.getenv("VERTEX_PROJECT_ID")
        location = os.getenv("VERTEX_LOCATION", "us-central1")
        vertexai.init(project=project, location=location)
        model = GenerativeModel(os.getenv("VERTEX_MODEL", "gemini-1.5-flash"))
        response = model.generate_content(
            f"Summarize the following webpage content in 2-4 concise paragraphs. Focus on the main topics, key information, and purpose of the page.\n\n{text}",
            generation_config={"max_output_tokens": 500},
        )
        if response and response.text:
            return response.text.strip()
    except ImportError:
        logger.warning("Vertex AI SDK not installed; pip install google-cloud-aiplatform")
    return None
