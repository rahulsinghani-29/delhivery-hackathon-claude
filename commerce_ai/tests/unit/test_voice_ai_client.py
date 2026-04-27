"""Unit tests for communication/voice_ai_client.py — GeminiVoiceClient."""

from __future__ import annotations

import pytest

from communication.voice_ai_client import (
    GeminiVoiceClient,
    CORE_PROMPT,
    ADDRESS_RULES,
    COD_TO_PREPAID_RULES,
    VOICE_PROFILES,
    get_voice_profile,
)


@pytest.fixture
def client():
    return GeminiVoiceClient(api_key=None)


def _order(**overrides):
    base = {
        "order_id": "ORD001",
        "merchant_id": "M001",
        "customer_ucid": "CUST001",
        "customer_name": "Rajesh Sharma",
        "merchant_name": "Amazon",
        "product": "T-shirt",
        "category": "fashion",
        "order_value": "3000",
        "current_address": "Central Park 2, Sohna Road, Gurgaon 122018",
        "destination_pincode": "122018",
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------------
# Voice profiles
# ------------------------------------------------------------------

class TestVoiceProfiles:
    def test_profiles_have_required_fields(self):
        for p in VOICE_PROFILES:
            assert "id" in p
            assert "name" in p
            assert "gender" in p

    def test_get_voice_profile_female_professional(self):
        p = get_voice_profile("female", "professional")
        assert p["id"] == "Kore"
        assert p["name"] == "Priya"

    def test_get_voice_profile_male_enthusiastic(self):
        p = get_voice_profile("male", "enthusiastic")
        assert p["id"] == "Puck"
        assert p["name"] == "Rohan"

    def test_get_voice_profile_fallback(self):
        p = get_voice_profile("unknown", "unknown")
        assert p is not None  # Should return default

    def test_class_voice_profiles_dict(self):
        """VOICE_PROFILES class attribute has expected structure."""
        profiles = GeminiVoiceClient.VOICE_PROFILES
        assert "professional_female" in profiles
        assert "enthusiastic_male" in profiles
        assert "empathetic_female" in profiles
        assert "confident_male" in profiles
        for key, val in profiles.items():
            assert "gemini_id" in val
            assert "name" in val


# ------------------------------------------------------------------
# System prompt generation
# ------------------------------------------------------------------

class TestBuildSystemPrompt:
    def test_address_enrichment_prompt_contains_core_rules(self, client):
        prompt = client.build_system_prompt("address_enrichment", _order())
        # Project Echo CORE behavioral rules
        assert "Indian accent" in prompt
        assert "MULTILINGUAL" in prompt
        assert "brisk" in prompt.lower() or "TEMPO" in prompt
        assert "[CALL_END]" in prompt
        assert "NEVER read AWB" in prompt

    def test_address_enrichment_prompt_contains_address_rules(self, client):
        prompt = client.build_system_prompt("address_enrichment", _order())
        assert "House/Flat" in prompt
        assert "Floor" in prompt
        assert "Tower" in prompt
        assert "Landmark" in prompt
        assert "Confirm each" in prompt

    def test_address_enrichment_prompt_references_order(self, client):
        prompt = client.build_system_prompt("address_enrichment", _order())
        assert "Amazon" in prompt
        assert "T-shirt" in prompt
        assert "Rajesh" in prompt

    def test_cod_to_prepaid_prompt_contains_core_rules(self, client):
        prompt = client.build_system_prompt("cod_to_prepaid", _order())
        assert "Indian accent" in prompt
        assert "MULTILINGUAL" in prompt
        assert "[CALL_END]" in prompt

    def test_cod_to_prepaid_prompt_contains_conversion_rules(self, client):
        prompt = client.build_system_prompt("cod_to_prepaid", _order())
        assert "5%" in prompt or "discount" in prompt.lower()
        assert "prepaid" in prompt.lower()

    def test_cod_to_prepaid_prompt_references_order(self, client):
        prompt = client.build_system_prompt("cod_to_prepaid", _order())
        assert "Amazon" in prompt
        assert "3000" in prompt

    def test_prompt_contains_indian_language_list(self, client):
        prompt = client.build_system_prompt("address_enrichment", _order())
        assert "Hindi" in prompt
        assert "Bengali" in prompt
        assert "Tamil" in prompt

    def test_prompt_contains_wrapup_rules(self, client):
        prompt = client.build_system_prompt("address_enrichment", _order())
        assert "wrap-up" in prompt.lower() or "goodbye" in prompt.lower()


# ------------------------------------------------------------------
# Phased prompt builder (build_call_prompt)
# ------------------------------------------------------------------

class TestBuildCallPrompt:
    def test_returns_three_phases(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        assert "phase1" in result
        assert "phase2" in result
        assert "phase3" in result
        assert isinstance(result["phase1"], str)
        assert isinstance(result["phase2"], str)
        assert isinstance(result["phase3"], str)

    def test_phase1_contains_core_behavioral_rules(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        p1 = result["phase1"]
        assert "Indian accent" in p1
        assert "MULTILINGUAL" in p1
        assert "TEMPO" in p1
        assert "[CALL_END]" in p1
        assert "NEVER read AWB" in p1

    def test_phase1_contains_greeting_for_address(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        p1 = result["phase1"]
        assert "Amazon" in p1
        assert "T-shirt" in p1
        assert "confirm your delivery address" in p1

    def test_phase1_contains_greeting_for_cod(self, client):
        result = client.build_call_prompt("cod_to_prepaid", _order())
        p1 = result["phase1"]
        assert "cash on delivery" in p1
        assert "special offer" in p1

    def test_phase2_address_contains_rules(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        p2 = result["phase2"]
        assert "House/Flat" in p2
        assert "Floor" in p2
        assert "Tower" in p2
        assert "Landmark" in p2
        assert "INCOMPLETE" in p2

    def test_phase2_cod_contains_rules(self, client):
        result = client.build_call_prompt("cod_to_prepaid", _order())
        p2 = result["phase2"]
        assert "5%" in p2 or "discount" in p2.lower()
        assert "prepaid" in p2.lower()
        assert "UPI" in p2

    def test_phase3_contains_wrapup(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        p3 = result["phase3"]
        assert "wrap-up" in p3.lower()
        assert "[CALL_END]" in p3

    def test_phase1_has_begin_speaking_instruction(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        assert "Begin speaking IMMEDIATELY" in result["phase1"]

    def test_phase2_has_system_update_header(self, client):
        result = client.build_call_prompt("address_enrichment", _order())
        assert "[SYSTEM UPDATE" in result["phase2"]


# ------------------------------------------------------------------
# Call initiation
# ------------------------------------------------------------------

class TestInitiateCall:
    def test_returns_expected_fields(self, client):
        result = client.initiate_call(
            "CUST001", "address_enrichment",
            {"order_summary": _order()},
        )
        assert "call_id" in result
        assert "status" in result
        assert "resolution" in result
        assert "transcript_summary" in result
        assert result["call_id"].startswith("gemini_")

    def test_call_status_is_valid(self, client):
        for _ in range(20):
            result = client.initiate_call(
                "CUST001", "address_enrichment",
                {"order_summary": _order()},
            )
            assert result["status"] in ("completed", "no_answer", "failed")

    def test_completed_call_has_resolution(self, client):
        for _ in range(50):
            result = client.initiate_call(
                "CUST001", "address_enrichment",
                {"order_summary": _order()},
            )
            if result["status"] == "completed":
                assert result["resolution"] is not None
                assert result["transcript_summary"] is not None
                return

    def test_address_enrichment_resolution_type(self, client):
        for _ in range(50):
            result = client.initiate_call(
                "CUST001", "address_enrichment",
                {"order_summary": _order()},
            )
            if result["status"] == "completed":
                assert result["resolution"] == "address_updated"
                return

    def test_cod_to_prepaid_resolution_type(self, client):
        for _ in range(50):
            result = client.initiate_call(
                "CUST001", "cod_to_prepaid",
                {"order_summary": _order()},
            )
            if result["status"] == "completed":
                assert result["resolution"] == "payment_converted"
                return


# ------------------------------------------------------------------
# Call status
# ------------------------------------------------------------------

class TestGetCallStatus:
    def test_known_call(self, client):
        call = client.initiate_call(
            "CUST001", "address_enrichment",
            {"order_summary": _order()},
        )
        status = client.get_call_status(call["call_id"])
        assert status["call_id"] == call["call_id"]
        assert status["status"] == call["status"]

    def test_unknown_call(self, client):
        status = client.get_call_status("nonexistent_call")
        assert status["status"] == "unknown"


# ------------------------------------------------------------------
# Call outcome extraction
# ------------------------------------------------------------------

class TestExtractCallOutcome:
    def test_address_extraction_with_flat(self, client):
        transcripts = [
            {"role": "assistant", "text": "Can you tell me your flat number?"},
            {"role": "user", "text": "Flat 1203"},
            {"role": "assistant", "text": "Flat 1203, right? And which floor?"},
            {"role": "user", "text": "12th floor"},
            {"role": "assistant", "text": "Tower number?"},
            {"role": "user", "text": "Tower B"},
            {"role": "assistant", "text": "Tower B, right? Any landmark nearby?"},
            {"role": "user", "text": "Near City Mall"},
        ]
        result = client.extract_call_outcome(transcripts, "address_enrichment", _order())
        assert result["resolution"] == "address_updated"
        data = result["extracted_data"]
        assert data["house_flat"] == "1203"
        assert data["floor"] == "12"
        assert data["tower_block"] == "B"

    def test_address_extraction_empty_transcripts(self, client):
        result = client.extract_call_outcome([], "address_enrichment", _order())
        assert result["resolution"] == "no_resolution"

    def test_cod_conversion_accepted(self, client):
        transcripts = [
            {"role": "assistant", "text": "Would you like to switch to prepaid?"},
            {"role": "user", "text": "Yes, sure, I'll pay now"},
            {"role": "assistant", "text": "Payment link sent via SMS"},
        ]
        result = client.extract_call_outcome(transcripts, "cod_to_prepaid", _order())
        assert result["resolution"] == "payment_converted"
        assert result["extracted_data"]["decision"] == "converted"

    def test_cod_conversion_declined(self, client):
        transcripts = [
            {"role": "assistant", "text": "Would you like to switch to prepaid?"},
            {"role": "user", "text": "No, I want cash on delivery"},
        ]
        result = client.extract_call_outcome(transcripts, "cod_to_prepaid", _order())
        assert result["resolution"] == "no_resolution"
        assert result["extracted_data"]["decision"] == "declined"

    def test_cod_conversion_unknown(self, client):
        transcripts = [
            {"role": "assistant", "text": "Hello, are you there?"},
        ]
        result = client.extract_call_outcome(transcripts, "cod_to_prepaid", _order())
        assert result["extracted_data"]["decision"] == "unknown"

    def test_unknown_issue_type(self, client):
        result = client.extract_call_outcome([], "unknown", _order())
        assert result["resolution"] == "no_resolution"
