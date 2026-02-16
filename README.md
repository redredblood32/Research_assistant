# Research Assistant (Streamlit)

An end-to-end research assistant that plans a topic outline, generates academic search queries, pulls papers, ranks relevance, and produces an abstract-based report. Built with Streamlit and designed to work with either a local Ollama model or a hosted GLM (Z.ai/OpenBigModel) endpoint.

**Quick Links**
- Query framing examples: `QUERY_GUIDE.md`

## Features
- LLM-assisted topic planning and query generation
- Academic search with relevance scoring
- PDF and RIS handling with download tracking
- Session/project persistence with exports to Excel
- Optional Chrome automation for PDF retrieval

## Requirements
- Python 3.x
- Internet access
- Chrome or Chromium installed (used by Selenium for PDF handling)
- Optional: Ollama installed and running locally

## Install & Run

### macOS (double-click)
- `mac/install_macos.command`
- `mac/run_macos.command`

### macOS (terminal)
```bash
bash mac/install_macos.command
bash mac/run_macos.command
```

### Windows (double-click)
- `windows\install_win.cmd`
- `windows\run_win.cmd`

### Windows (terminal)
```bat
windows\install_win.cmd
windows\run_win.cmd
```

### Manual (all platforms)
```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate.bat  # Windows
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
streamlit run Research_assistant_v1.py
```

## Configuration

### API Keys (configured in the app UI)
The sidebar includes inputs for:
- Semantic Scholar API Key (improves paper search and metadata)
- OpenAlex Email (polite rate limiting)
- Elsevier API Key (PDF fallback access when available)
- GLM API Key (for Z.ai/OpenBigModel)

### LLM Providers
- **Ollama (local)**: set the Ollama URL in the app and ensure your model is running.
- **GLM (hosted)**: provide the API key and base URL in the app.

### Data Directory
By default the app stores data in `research_assistant_data/` inside the repo.  
You can override this with:
```
RESEARCH_ASSISTANT_DATA_ROOT=/path/to/data
```

## Usage
1. Enter a **Research Topic** in the main field.
2. Click **Start Research** to run the agent flow.
3. Review the generated plan, search queries, and ranked papers.
4. Export results to Excel using **Export Full Excel**.

For topic framing examples and best practices, see `QUERY_GUIDE.md`.

## Output Locations
- `research_assistant_data/projects/` stores projects and sessions
- Excel exports are saved per session inside the project folder
- PDFs and RIS files are stored under the project’s data directories

## Troubleshooting
- **Missing API keys**: The app will warn you in the sidebar if a key is required.
- **Chrome / Selenium issues**: Ensure Chrome/Chromium is installed and up to date.
- **Port conflicts**: Streamlit defaults to port 8501. Stop other instances or run:
  ```
  streamlit run Research_assistant_v1.py --server.port 8502
  ```
- **Ollama connection errors**: Confirm the Ollama server is running and the URL is correct.

## Repo Layout
- `Research_assistant_v1.py` – main Streamlit app
- `requirements.txt` – Python dependencies
- `QUERY_GUIDE.md` – query framing examples
- `mac/` – macOS install/run scripts
- `windows/` – Windows install/run scripts

## License
Add your license information here.
