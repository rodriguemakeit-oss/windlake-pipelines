from typing import Dict, Any, List
from string import Template

SERIAL_NUMBER_SEP = "|"


def generate_completion_prompt(prompt_config: Dict[str, Any], already_ids: List[str]) -> str:
    """Follow-up user turn asking only for the turbines missing from the first answer."""
    template = prompt_config.get("generic_prompt").get("completion")
    return Template(template).substitute(already=" ; ".join(already_ids))



def get_custom_prompt_part(
    custom_prompt_parts: Dict[str, str], part_name: str, generic_prompt_part: str
) -> str:
    if custom_prompt_parts is None:
        return generic_prompt_part

    custom_prompt_part = custom_prompt_parts.get(part_name, None)
    if custom_prompt_part is None:
        return generic_prompt_part
    else:
        prompt = (
            generic_prompt_part + " " + custom_prompt_part["text"]
            if custom_prompt_part.get("extra", None)
            else custom_prompt_part["text"]
        )
        return prompt


def generate_system_prompt_di(
    prompt_config: Dict[str, Any], maintainer_custom_prompt_parts: Dict[str, str] = None
):
    print("   ├─ 📝 Generating system prompt")
    generic_prompt = prompt_config.get("generic_prompt")
    main_system_prompt = generic_prompt.get("system").get("main")
    lego_system_prompt = generic_prompt.get("system").get("lego")
    turbine_id = Template(lego_system_prompt.get("turbine_id")).substitute(
        SERIAL_NUMBER_SEP=SERIAL_NUMBER_SEP
    )
    turbine_id = get_custom_prompt_part(
        custom_prompt_parts=maintainer_custom_prompt_parts,
        part_name="turbine_id",
        generic_prompt_part=turbine_id,
    )

    how_to_find_availability = lego_system_prompt.get("how_to_find_availability")
    how_to_find_availability = get_custom_prompt_part(
        custom_prompt_parts=maintainer_custom_prompt_parts,
        part_name="how_to_find_availability",
        generic_prompt_part=how_to_find_availability,
    )

    where_to_find_availability = lego_system_prompt.get("where_to_find_availability")
    where_to_find_availability = get_custom_prompt_part(
        custom_prompt_parts=maintainer_custom_prompt_parts,
        part_name="where_to_find_availability",
        generic_prompt_part=where_to_find_availability,
    )

    customized_prompt_parts = {
        "turbine_id": turbine_id,
        "where_to_find_availability": where_to_find_availability,
        "how_to_find_availability": how_to_find_availability,
    }

    return Template(main_system_prompt).substitute(**customized_prompt_parts)


def generate_user_prompt(
    prompt_config: Dict[str, Any],
    data: str,
    file_type: str = "pdf",
    report_year: int = None,
    report_month: int = None,
):
    print("   ├─ 💬 Generating user prompt")
    generic_prompt = prompt_config.get("generic_prompt")
    main_user_prompt = generic_prompt.get("user")
    prompt = Template(main_user_prompt).substitute(data=data, file_type=file_type)
    if report_year and report_month:
        prompt += f"\n<report_period>report_year={report_year}, report_month={report_month:02d}</report_period>"
    return prompt
