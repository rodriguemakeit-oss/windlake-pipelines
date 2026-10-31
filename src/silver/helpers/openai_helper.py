from openai import AzureOpenAI, ChatCompletion

openai_api_version = "2024-02-01"


def get_openai_client(
    openai_key: str, openai_endpoint: str, openai_api_version: str = openai_api_version
) -> AzureOpenAI:
    """
    Return Azure OpenAI client.

    Returns:
        AzureOpenAI: client.
    """
    return AzureOpenAI(
        api_key=openai_key,
        azure_endpoint=openai_endpoint,
        api_version=openai_api_version,
    )


def call_model(
    client: AzureOpenAI,
    system_prompt: str,
    user_prompt: str,
    model: str,
) -> ChatCompletion:
    """
    Calls the Azure OpenAI model to generate a chat completion based on the provided prompts.

    Args:
        client (AzureOpenAI): The Azure OpenAI client instance.
        system_prompt (str): The system prompt to guide the model's behavior.
        user_prompt (str): The user prompt to generate a response for.
        model (str, optional): The model to use for generating the completion.

    Returns:
        ChatCompletion: The generated chat completion from the model.
    """
    return client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )


def call_model_followup(
    client: AzureOpenAI,
    system_prompt: str,
    user_prompt: str,
    assistant_answer: str,
    followup_prompt: str,
    model: str,
) -> ChatCompletion:
    """Continue the extraction conversation with one more user turn.

    Replays (system, user=data, assistant=first answer) so the model sees what it
    already returned, then asks ``followup_prompt`` (e.g. the missing turbines).
    """
    return client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": assistant_answer},
            {"role": "user", "content": followup_prompt},
        ],
    )
