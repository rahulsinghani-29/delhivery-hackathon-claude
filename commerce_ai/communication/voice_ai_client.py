"""Gemini-based voice AI client for outbound customer calls.

Ported from Project Echo. Uses Google Gemini's Live API for real-time
voice conversations with customers about address enrichment and
COD-to-prepaid conversion.
"""

from __future__ import annotations

import os
import re
import uuid
import random
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Voice profiles — mapped from Project Echo's voices.ts
# ---------------------------------------------------------------------------

VOICE_PROFILES = [
    {"id": "Kore", "name": "Priya", "personality": "Calm & professional", "gender": "female"},
    {"id": "Aoede", "name": "Ananya", "personality": "Bright & energetic", "gender": "female"},
    {"id": "Leda", "name": "Meera", "personality": "Warm & empathetic", "gender": "female"},
    {"id": "Charon", "name": "Arjun", "personality": "Confident & direct", "gender": "male"},
    {"id": "Puck", "name": "Rohan", "personality": "Friendly & upbeat", "gender": "male"},
    {"id": "Orus", "name": "Vikram", "personality": "Composed & reassuring", "gender": "male"},
    {"id": "Fenrir", "name": "Kabir", "personality": "Deep & authoritative", "gender": "male"},
    {"id": "Zephyr", "name": "Aarav", "personality": "Light & conversational", "gender": "male"},
]

VOICE_BY_TONE = {
    "female": {"professional": "Kore", "enthusiastic": "Aoede", "empathetic": "Leda"},
    "male": {"professional": "Charon", "enthusiastic": "Puck", "empathetic": "Orus"},
}

DEFAULT_VOICE_ID = "Kore"  # Priya — calm & professional


def get_voice_profile(gender: str = "female", tone: str = "professional") -> dict:
    """Return a voice profile matching the requested gender and tone."""
    vid = VOICE_BY_TONE.get(gender, {}).get(tone, DEFAULT_VOICE_ID)
    return next((v for v in VOICE_PROFILES if v["id"] == vid), VOICE_PROFILES[0])


# ---------------------------------------------------------------------------
# CORE behavioural prompt — ported from Project Echo voice-prompts.ts
# ---------------------------------------------------------------------------

CORE_PROMPT = """\
You are a Delhivery customer support agent on a phone call. You are a real \
person from India, not AI.

ACCENT — THIS IS CRITICAL: You MUST speak with an Indian accent throughout \
the ENTIRE call. You are an Indian person from Delhi. Pronounce words the way \
Indians do. Do NOT switch to American or British accent at any point. Stay \
consistently Indian. This must not change mid-conversation.

LANGUAGE — CRITICAL: You are MULTILINGUAL. Start the call in English with an \
Indian accent. But if the customer speaks in Hindi, Bengali, Tamil, Telugu, \
Marathi, Kannada, Malayalam, Gujarati, Odia, Punjabi, Assamese, Urdu, or any \
other Indian language — IMMEDIATELY switch to that language and continue the \
ENTIRE conversation in that language. Match the customer's language. If they \
mix languages (Hinglish, Benglish, etc.), match their style. NEVER insist on \
English. NEVER say "please speak in English". The customer's language \
preference is always respected. You are fluent in all major Indian languages.

GREETING — say IMMEDIATELY when call connects, within 3 seconds:
"Hi, this is {agent_name} from Delhivery, calling about your recent \
{product} order from {seller}."
Then state the PURPOSE of the call based on the scenario. Do NOT default to \
"confirm your address" — say what the call is actually about.

TEMPO — THIS IS CRITICAL: Speak at a natural, brisk pace — like a \
professional making a quick but friendly call. Keep sentences short and \
punchy. No filler words. No "umm" or "so". Move from one topic to the next \
smoothly. Pause briefly after asking a question to let the customer respond. \
Do NOT rush through the conversation — be efficient but conversational.

STYLE: 1-2 short sentences max. ONE question at a time. Say flat number, \
kindly, pincode.
MISHEAR: "Sorry, could you repeat that?" — never guess.
CONFIRM: Repeat back key info: "Flat 1203, right?"
INTERRUPT: Stop talking immediately, listen, respond.
NOISE: Ignore background noise. Respond only to clear speech.
SILENCE: If the customer is silent for more than 8-10 seconds, say "Hello, \
are you there?" Do NOT say this after short pauses — give the customer time \
to think and respond.
NEVER read AWB/order IDs. Say "your order from {seller}".
DONE: After goodbye, say [CALL_END].
REMINDER: Maintain Indian accent at ALL times. Never drift to American \
accent. Keep the pace brisk but natural. If the customer speaks in any Indian \
language, switch to that language immediately.
"""

# ---------------------------------------------------------------------------
# Issue-specific prompt blocks — ported from Project Echo
# ---------------------------------------------------------------------------

ADDRESS_RULES = """\
ADDRESS: Collect one at a time: House/Flat, Floor, Tower/Block, Society, \
Street, Sector, Landmark, City, State, Pincode.
Skip fields you already have. Confirm each: "Tower 3, right?" For landmark: \
"Any landmark nearby, like a mall or school?" Read full address at end.
IMPORTANT: Before saying goodbye, ALWAYS read back the complete confirmed \
address to the customer. Do NOT end the call without confirming the full \
address. Wait for the customer to confirm before wrapping up.
"""

COD_TO_PREPAID_RULES = """\
COD-TO-PREPAID CONVERSION:
1. Greet the customer. Reference their COD order from the seller.
2. Offer a 5% discount if they switch to prepaid payment now.
3. Mention UPI, Paytm, PhonePe as payment options.
4. If interested, tell them a secure payment link has been sent via SMS.
5. If not interested, respect their choice and confirm COD delivery will \
proceed as normal.
6. Thank them and say goodbye. Then say [CALL_END].
"""

WRAPUP_RULES = """\
[SYSTEM UPDATE — wrap-up instructions]
When the customer confirms the final address or says goodbye:
1. Read the FULL confirmed address back to the customer in one go.
2. Summarize: the decision made + the confirmed address.
3. Thank them and say goodbye warmly.
4. After goodbye, say [CALL_END] to signal the call is over.
"""


class GeminiVoiceClient:
    """Gemini Live API voice client adapted from Project Echo.

    Uses Google's Gemini multimodal API for real-time voice conversations.
    For the hackathon, this is a server-side mock that simulates the call flow
    and returns structured results. The actual Gemini WebSocket integration
    runs in the frontend (Project Echo's useGeminiVoice hook).
    """

    # Voice profiles from Project Echo's voices.ts
    VOICE_PROFILES = {
        "professional_female": {"gemini_id": "Kore", "name": "Priya"},
        "enthusiastic_male": {"gemini_id": "Puck", "name": "Rohan"},
        "empathetic_female": {"gemini_id": "Leda", "name": "Meera"},
        "confident_male": {"gemini_id": "Charon", "name": "Arjun"},
    }

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "gemini-2.0-flash-live",
    ) -> None:
        """Initialize with Gemini API key. Falls back to mock if no key."""
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        self.model = model
        self._calls: dict[str, dict] = {}  # in-memory call store for mock

    # ------------------------------------------------------------------
    # Phased prompt builder — ported from Project Echo voice-prompts.ts
    # ------------------------------------------------------------------

    def build_call_prompt(self, issue_type: str, order: dict) -> dict:
        """Build the phased prompt for a Gemini voice call.

        Adapted from Project Echo's voice-prompts.ts:
        - Phase 1: Core behavioral rules + minimal greeting (fast TTFT)
        - Phase 2: Scenario-specific rules + order details
        - Phase 3: Wrap-up instructions

        Returns: { phase1: str, phase2: str, phase3: str }
        """
        profile = get_voice_profile("female", "professional")
        agent_name = profile["name"]
        seller = order.get("merchant_name", order.get("client", "the seller"))
        product = order.get("product", order.get("category", "order"))
        customer_first = (
            order.get("customer_name", "Customer").split()[0]
            if order.get("customer_name")
            else "Customer"
        )

        # Phase 1 — core behavioural block + minimal greeting (fast TTFT)
        phase1 = CORE_PROMPT.format(
            agent_name=agent_name,
            product=product,
            seller=seller,
        )
        phase1 += (
            "Begin speaking IMMEDIATELY when the session starts. "
            "Do not wait for the customer to speak first.\n"
        )
        phase1 += f"Customer: {customer_first}. Product: {product}. Seller: {seller}.\n"

        if issue_type == "address_enrichment":
            phase1 += (
                f'In your greeting say: "Hi, this is {agent_name} from Delhivery. '
                f"I'm calling about your recent {product} order from {seller}. "
                f'I just need to quickly confirm your delivery address."\n'
            )
        elif issue_type == "cod_to_prepaid":
            phase1 += (
                f'In your greeting say: "Hi, this is {agent_name} from Delhivery. '
                f"I'm calling about your recent {product} order from {seller}. "
                f"Since it's a cash on delivery order, I wanted to let you know "
                f'about a special offer."\n'
            )

        # Phase 2 — scenario-specific rules + order details
        phase2 = "[SYSTEM UPDATE — follow these additional instructions for this call]\n\n"

        if issue_type == "address_enrichment":
            phase2 += f"Scenario: Address Verification\nTask: Confirm and enrich delivery address\n\n"
            phase2 += ADDRESS_RULES
            current_address = order.get("current_address", order.get("destination_pincode", ""))
            phase2 += (
                f"\nFull order details:\n"
                f"Customer: {order.get('customer_name', 'Customer')} | "
                f"Product: {product} | Seller: {seller}\n"
                f"Address on file: {current_address} "
                f"(INCOMPLETE — need flat, floor, tower, landmark)\n"
                f'Call them "{customer_first}". '
                f'Say "your {product} from {seller}".\n'
            )
        elif issue_type == "cod_to_prepaid":
            order_value = order.get("order_value", "")
            phase2 += f"Scenario: COD to Prepaid Conversion\nTask: Offer prepaid conversion with discount\n\n"
            phase2 += COD_TO_PREPAID_RULES
            phase2 += (
                f"\nFull order details:\n"
                f"Customer: {order.get('customer_name', 'Customer')} | "
                f"Product: {product} | Seller: {seller} | COD ₹{order_value}\n"
                f'Call them "{customer_first}". '
                f'Say "your {product} from {seller}".\n'
            )

        # Phase 3 — wrap-up instructions
        phase3 = WRAPUP_RULES

        return {"phase1": phase1, "phase2": phase2, "phase3": phase3}

    # ------------------------------------------------------------------
    # System prompt builder — ported from Project Echo
    # ------------------------------------------------------------------

    def build_system_prompt(self, issue_type: str, order: dict) -> str:
        """Build the Gemini system prompt based on issue type.

        Ported from Project Echo's buildPhase1 / buildPhase2 in
        voice-prompts.ts and prompt-builder.ts.
        """
        profile = get_voice_profile("female", "professional")
        agent_name = profile["name"]
        seller = order.get("merchant_name", order.get("client", "the seller"))
        product = order.get("product", order.get("category", "order"))
        customer_first = (
            order.get("customer_name", "Customer").split()[0]
            if order.get("customer_name")
            else "Customer"
        )

        # Phase 1 — core behavioural block
        prompt = CORE_PROMPT.format(
            agent_name=agent_name,
            product=product,
            seller=seller,
        )

        prompt += f"\nCustomer: {customer_first}. Product: {product}. Seller: {seller}.\n"

        # Phase 2 — issue-specific rules
        if issue_type == "address_enrichment":
            prompt += f"\nScenario: Address Verification\n"
            prompt += (
                f'In your greeting say: "Hi, this is {agent_name} from Delhivery. '
                f"I'm calling about your recent {product} order from {seller}. "
                f'I just need to quickly confirm your delivery address."\n\n'
            )
            prompt += ADDRESS_RULES

            current_address = order.get("current_address", order.get("destination_pincode", ""))
            prompt += (
                f"\nAddress on file: {current_address} (INCOMPLETE — need flat, "
                f"floor, tower, landmark)\n"
                f'Call them "{customer_first}". '
                f'Say "your {product} from {seller}".\n'
            )

        elif issue_type == "cod_to_prepaid":
            order_value = order.get("order_value", "")
            prompt += f"\nScenario: COD to Prepaid Conversion\n"
            prompt += (
                f'In your greeting say: "Hi, this is {agent_name} from Delhivery. '
                f"I'm calling about your recent {product} order from {seller}. "
                f"Since it's a cash on delivery order worth ₹{order_value}, "
                f"I wanted to let you know about a special offer — switch to "
                f'prepaid now and get 5% off!"\n\n'
            )
            prompt += COD_TO_PREPAID_RULES
            prompt += (
                f'\nCall them "{customer_first}". '
                f'Say "your {product} from {seller}".\n'
            )

        # Phase 3 — wrap-up
        prompt += "\n" + WRAPUP_RULES

        return prompt

    # ------------------------------------------------------------------
    # Call initiation
    # ------------------------------------------------------------------

    def initiate_call(
        self,
        customer_ucid: str,
        issue_type: str,
        call_context: dict,
    ) -> dict:
        """Initiate a Gemini voice call.

        For hackathon: mock that simulates the call flow.
        In production: would use Gemini Live API WebSocket
        (as in Project Echo's useGeminiVoice.ts).

        Returns: { call_id, status, resolution, transcript_summary }
        """
        call_id = f"gemini_{uuid.uuid4().hex[:12]}"
        order = call_context.get("order_summary", call_context)

        # Build the system prompt (would be sent to Gemini in production)
        _system_prompt = self.build_system_prompt(issue_type, order)

        # Mock call outcome
        roll = random.random()
        if roll < 0.5:
            status = "completed"
            if issue_type == "address_enrichment":
                resolution = "address_updated"
                transcript_summary = (
                    "Customer confirmed address: Flat 1203, 12th Floor, "
                    "Tower B, near City Mall. Address updated."
                )
            else:
                resolution = "payment_converted"
                transcript_summary = (
                    "Customer agreed to switch to prepaid. Payment link "
                    "sent via SMS. Customer confirmed receipt."
                )
        elif roll < 0.8:
            status = "no_answer"
            resolution = None
            transcript_summary = None
        else:
            status = "failed"
            resolution = None
            transcript_summary = None

        call_record = {
            "call_id": call_id,
            "status": status,
            "resolution": resolution,
            "transcript_summary": transcript_summary,
            "customer_ucid": customer_ucid,
            "issue_type": issue_type,
            "started_at": datetime.utcnow().isoformat(),
        }
        self._calls[call_id] = call_record

        return {
            "call_id": call_id,
            "status": status,
            "resolution": resolution,
            "transcript_summary": transcript_summary,
        }

    # ------------------------------------------------------------------
    # Call status
    # ------------------------------------------------------------------

    def get_call_status(self, call_id: str) -> dict:
        """Get status of an ongoing/completed call."""
        record = self._calls.get(call_id)
        if record:
            return {
                "call_id": call_id,
                "status": record["status"],
                "duration_seconds": random.randint(30, 180) if record["status"] == "completed" else None,
                "resolution": record.get("resolution"),
            }
        # Unknown call — return generic status
        return {
            "call_id": call_id,
            "status": "unknown",
            "duration_seconds": None,
            "resolution": None,
        }

    # ------------------------------------------------------------------
    # Call outcome extraction — ported from Project Echo call-extraction.ts
    # ------------------------------------------------------------------

    def extract_call_outcome(
        self,
        transcripts: list[dict],
        issue_type: str,
        order: dict,
    ) -> dict:
        """Extract structured outcome from call transcripts.

        Ported from Project Echo's extractAddress / extractNDR in
        call-extraction.ts. Uses regex-based parsing on transcript text.

        For address_enrichment: extract address fields
        For cod_to_prepaid: extract conversion decision

        Returns: { resolution, extracted_data }
        """
        if issue_type == "address_enrichment":
            return self._extract_address_outcome(transcripts, order)
        elif issue_type == "cod_to_prepaid":
            return self._extract_cod_outcome(transcripts)
        return {"resolution": "no_resolution", "extracted_data": {}}

    def _extract_address_outcome(
        self, transcripts: list[dict], order: dict
    ) -> dict:
        """Extract address fields from call transcripts.

        Ported from Project Echo's extractAddress() — regex-based parsing
        of flat/house, floor, tower/block, landmark from conversation text.
        """
        user_text = " ".join(
            t.get("text", "") for t in transcripts if t.get("role") == "user"
        )
        bot_text = " ".join(
            t.get("text", "") for t in transcripts if t.get("role") == "assistant"
        )
        combined = bot_text + " " + user_text

        # House/Flat number
        house_flat = ""
        for pattern in [
            r"[Ff]lat\s*(?:number\s*)?(?:is\s*)?(\d+[A-Za-z]?(?:[-/]\d+)?)",
            r"[Hh]ouse\s*(?:number\s*)?(?:is\s*)?(\d+[A-Za-z]?(?:[-/]\d+)?)",
        ]:
            m = re.search(pattern, combined, re.IGNORECASE)
            if m:
                house_flat = m.group(1).strip()
                break
        if not house_flat:
            m = re.search(r"\b([A-Z]-?\d+[A-Za-z]?)\b", user_text)
            if m:
                house_flat = m.group(1).strip()

        # Floor
        floor = ""
        m = re.search(r"(\d+)\s*(?:st|nd|rd|th)\s*[Ff]loor", combined, re.IGNORECASE)
        if m:
            floor = m.group(1)
        elif re.search(r"[Gg]round\s*[Ff]loor", combined, re.IGNORECASE):
            floor = "Ground"

        # Tower/Block
        tower = ""
        tower_confirms = list(re.finditer(
            r"[Tt]ower\s*(?:number\s*)?(?:is\s*)?([A-Za-z0-9]+)"
            r"(?:\s*[,.?!]|\s+right|\s+correct|$)",
            bot_text, re.IGNORECASE,
        ))
        if tower_confirms:
            val = tower_confirms[-1].group(1).strip()
            if len(val) <= 3 and val.upper() not in ("O", "THE", "IS"):
                tower = val.upper()
        if not tower:
            m = re.search(
                r"[Tt]ower\s*(?:number\s*)?(?:is\s*)?(\d+)", combined, re.IGNORECASE
            )
            if m:
                tower = m.group(1)

        # Landmark
        landmark = ""
        landmark_patterns = [
            r"[Ll]andmark\s+(?:is\s+)?([A-Za-z][A-Za-z0-9' ]{2,40}?)"
            r"(?:\s*[,.?!;]|\s+right|\s+correct|$)",
            r"[Nn]ear\s+([A-Z][A-Za-z0-9' ]{2,40}?)"
            r"(?:\s*[,.?!;]|\s+right|\s+correct|$)",
        ]
        for pat in landmark_patterns:
            matches = list(re.finditer(pat, bot_text, re.IGNORECASE))
            if matches:
                landmark = matches[-1].group(1).strip()
                break
        if not landmark:
            for pat in [
                r"[Nn]ear(?:by)?\s+(?:the\s+)?(?:to\s+)?([A-Za-z][A-Za-z0-9' ]{2,40}?)(?:\s*[,.?!]|$)",
                r"[Oo]pposite\s+(?:the\s+)?([A-Za-z][A-Za-z0-9' ]{2,40}?)(?:\s*[,.?!]|$)",
            ]:
                matches = list(re.finditer(pat, user_text, re.IGNORECASE))
                if matches:
                    landmark = matches[-1].group(1).strip()
                    break

        extracted = {
            "house_flat": house_flat,
            "floor": floor,
            "tower_block": tower,
            "landmark": landmark,
        }

        has_data = any(v for v in extracted.values())
        return {
            "resolution": "address_updated" if has_data else "no_resolution",
            "extracted_data": extracted,
        }

    def _extract_cod_outcome(self, transcripts: list[dict]) -> dict:
        """Extract COD-to-prepaid conversion decision from transcripts.

        Ported from Project Echo's prompt-builder.ts cod_to_prepaid flow.
        """
        user_text = " ".join(
            t.get("text", "").lower()
            for t in transcripts
            if t.get("role") == "user"
        )
        bot_text = " ".join(
            t.get("text", "").lower()
            for t in transcripts
            if t.get("role") == "assistant"
        )

        # Positive signals — customer wants to convert
        wants_prepaid = any(kw in user_text for kw in [
            "yes", "sure", "okay", "prepaid", "pay now", "switch",
            "interested", "haan", "theek hai", "kar do",
        ])
        bot_confirmed = any(kw in bot_text for kw in [
            "payment link", "sent via sms", "switched to prepaid",
            "converted", "payment processed",
        ])

        # Negative signals — customer declines
        # Use word-boundary check for short words like "no" to avoid
        # false positives (e.g. "now" containing "no").
        decline_keywords = [
            "nahi", "cod", "cash on delivery", "not interested",
            "don't want", "nahi chahiye",
        ]
        declines = any(kw in user_text for kw in decline_keywords) or bool(
            re.search(r"\bno\b", user_text)
        )

        if (wants_prepaid or bot_confirmed) and not declines:
            return {
                "resolution": "payment_converted",
                "extracted_data": {"decision": "converted"},
            }
        elif declines:
            return {
                "resolution": "no_resolution",
                "extracted_data": {"decision": "declined"},
            }
        return {
            "resolution": "no_resolution",
            "extracted_data": {"decision": "unknown"},
        }
