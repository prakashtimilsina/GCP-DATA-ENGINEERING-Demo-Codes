from google import genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch

# Option 1: API key from environment variable (recommended)
client = genai.Client()  # Uses GEMINI_API_KEY from environment

# Option 2: Pass API key directly (not recommended for production)
# client = genai.Client(api_key="your-gemini-api-key")

model_id = "gemini-2.5-flash-preview-05-20"

tools = [Tool(google_search=GoogleSearch())]  # Enables live Google Search

response = client.models.generate_content(
    model=model_id,
    contents="What is current date and time in Nepal?",
    config=GenerateContentConfig(
        tools=tools,
        response_modalities=["TEXT"],
    )
)
print(response.candidates[0].content.parts[0].text)