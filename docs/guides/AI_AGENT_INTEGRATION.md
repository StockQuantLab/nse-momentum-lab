# AI Agent Integration Design

**Goal**: Enable chat-based command execution instead of manual CLI

**Status**: Design Phase - Ready to Implement

---

## Overview

Instead of running commands manually:
```bash
# OLD WAY (manual)
doppler run -- uv run python -m nse_momentum_lab.services.ingest.worker 2025-02-07
```

You can ask the AI:
```
// NEW WAY (AI agent)
"Please ingest NSE data for 2025-02-07"
```

---

## Architecture

### Components

1. **Chat UI** (NiceGUI) - Already exists, needs enhancement
2. **Command Tools** (Python functions) - Safe, validated command execution
3. **Phidata Agent** - Orchestrates tool calls based on user intent
4. **Permission System** - Ensures only safe operations are allowed
5. **Feedback System** - Shows command output to user

### Data Flow

```
User → Chat Page → Phidata Agent → Tool Executor → Command Output → User
                  ↓
              Intent Recognition
                  ↓
              Tool Selection
                  ↓
              Permission Check
                  ↓
              Execution
                  ↓
              Result Formatting
```

---

## Phase 1: Safe Command Tools

### Tool Categories

#### 1. Read-Only Tools (Always Safe)
- `get_pipeline_status()` - Show recent job runs
- `get_scan_results(date)` - Show scan candidates
- `get_database_stats()` - Show database state
- `get_system_health()` - Check all services

#### 2. Write Tools (Require Confirmation)
- `run_ingestion(date)` - Download and store NSE data
- `run_adjustment()` - Apply corporate actions
- `run_scan(date)` - Execute scan for a date
- `run_rollup(date)` - Generate daily rollup

#### 3. Dangerous Tools (Blocked in Phase 1)
- Database schema changes
- Config modifications
- File system operations outside project

### Tool Implementation

```python
# src/nse_momentum_lab/agents/tools.py

from datetime import date
from typing import Any
import subprocess
import asyncio

class PipelineTools:
    """Safe pipeline command execution tools"""

    @staticmethod
    async def get_pipeline_status(limit: int = 10) -> dict[str, Any]:
        """Get recent pipeline job runs"""
        # Query database for recent jobs
        # Return formatted status
        pass

    @staticmethod
    async def run_ingestion(trading_date: date) -> dict[str, Any]:
        """Run ingestion for a specific date"""
        # Validate date is not in future
        # Validate date is a trading day
        # Execute: python -m nse_momentum_lab.services.ingest.worker
        # Return result with job ID
        pass

    @staticmethod
    async def run_scan(trading_date: date) -> dict[str, Any]:
        """Run scan for a specific date"""
        # Validate date
        # Check if data exists for that date
        # Execute scan worker
        # Return scan run ID
        pass
```

---

## Phase 2: Phidata Integration

### Agent Configuration

```python
# src/nse_momentum_lab/agents/assistant.py

from phi.agent import Agent
from phi.model.openai import OpenAIChat
from .tools import PipelineTools

def create_research_assistant() -> Agent:
    """Create AI assistant for pipeline operations"""

    tools = PipelineTools()

    agent = Agent(
        name="Pipeline Assistant",
        model=OpenAIChat(id="gpt-4"),
        instructions=[
            "You are a helpful assistant for nse-momentum-lab",
            "You can execute pipeline commands safely",
            "Always explain what you're going to do before executing",
            "Show results in a clear, formatted way",
            "If a command fails, explain why and suggest fixes",
            "Never execute destructive operations",
        ],
        tools=[
            tools.get_pipeline_status,
            tools.run_ingestion,
            tools.run_adjustment,
            tools.run_scan,
            tools.run_rollup,
        ],
        show_tool_calls=True,
        markdown=True,
    )

    return agent
```

### NiceGUI Integration

```python
# apps/nicegui/pages/chat.py (or integrated into daily_summary.py)

from nicegui import ui
from nse_momentum_lab.agents.agent import create_research_assistant

async def chat_page():
    ui.label("Chat with Pipeline Assistant").classes("text-2xl")

    # Initialize agent (persistent state)
    agent = create_research_assistant()

    # Chat interface
    with ui.row():
        prompt_input = ui.input(placeholder="Ask about the pipeline or request operations...")
        send_button = ui.button("Send")

    async def send_message():
        prompt = prompt_input.value
        if not prompt:
            return

        # Display user message
        with ui.row().classes("ml-4"):
            ui.label(f"You: {prompt}")

        # Get agent response
        with ui.row().classes("ml-4"):
            with ui.spinner("Thinking..."):
                response = await agent.run_async(prompt)
                ui.label(f"Assistant: {response}")

    send_button.on_click(send_message)
```

---

## Phase 3: Safety & Permissions

### Validation Rules

1. **Date Validation**
   - No future dates
   - Check against trading calendar
   - Validate data exists before dependent operations

2. **Idempotency**
   - All operations must be re-runnable
   - Check for existing data before running
   - Warn if data already exists

3. **Confirmation Required**
   - Write operations show what will happen
   - User can cancel before execution
   - Clear output showing job ID

4. **Rate Limiting**
   - Prevent multiple simultaneous operations
   - Queue operations if needed

### Error Handling

```python
async def safe_run_ingestion(date: date) -> dict[str, Any]:
    """Safely run ingestion with validation"""

    # Validate date
    if date > date.today():
        return {"error": "Cannot ingest future dates"}

    # Check if already ingested
    existing = await check_data_exists(date)
    if existing:
        return {
            "warning": f"Data already exists for {date}",
            "suggestion": "Re-run the requested date window; reserve --allow-full-rebuild for exceptional full rebuilds."
        }

    # Run ingestion
    try:
        result = await run_ingestion_worker(date)
        return {
            "status": "success",
            "job_id": result.job_id,
            "records_processed": result.count
        }
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "suggestion": "Check logs for details"
        }
```

---

## Implementation Plan

### Step 1: Create Tool Functions (Priority 1)
- [ ] Create `src/nse_momentum_lab/agents/tools.py`
- [ ] Implement read-only tools (status, stats)
- [ ] Implement write tools (ingest, scan, rollup)
- [ ] Add validation and error handling
- [ ] Write unit tests for tools

### Step 2: Integrate Phidata (Priority 2)
- [ ] Install phidata dependency
- [ ] Create agent configuration
- [ ] Test agent with read-only tools
- [ ] Test agent with write tools

### Step 3: Enhance Chat Page (Priority 3)
- [ ] Update Chat page to use agent
- [ ] Add session state for conversation history
- [ ] Format tool outputs nicely
- [ ] Add progress indicators
- [ ] Handle errors gracefully

### Step 4: Testing & Refinement (Priority 4)
- [ ] Test with real data ingestion
- [ ] Test error scenarios
- [ ] Add rate limiting
- [ ] Refine prompts and instructions
- [ ] User feedback and iteration

---

## Example Conversations

### Example 1: Check Status
```
User: "What's the pipeline status?"
AI: "Let me check the recent pipeline runs...

[Executes get_pipeline_status()]

Recent Jobs (last 10):
- scan_4p_2lynch: SUCCESS (2025-02-07, 45 candidates)
- backtest_exp_001: SUCCESS (2025-02-07)

Everything looks good! Would you like me to run a scan?"
```

### Example 2: Run Scan
```
User: "Please run a scan for 2025-02-07"
AI: "I'll run the scan for 2025-02-07.

This will:
1. Load market data from DuckDB/Parquet
2. Apply 4% breakout filter
3. Apply 2LYNCH filters

[Executes run_scan(date(2025-02-07))]

✅ Scan complete!
- Date: 2025-02-07
- Candidates found: 45
- Job ID: scan_20250207_abc123
- Duration: 5 seconds

Would you like me to run a backtest on these candidates?"
```

### Example 3: Error Handling
```
User: "Run the scan for 2025-02-08"
AI: "Let me run the scan for 2025-02-08...

[Executes run_scan(date(2025-02-08))]

❌ Scan failed:
Error: No data found for 2025-02-08 in md_ohlcv_adj

Suggestion: Please run ingestion and adjustment first before scanning.

Would you like me to:
1. Ingest data for 2025-02-08?
2. Check what dates are available?"
```

---

## Success Criteria

### Phase 1 Complete When:
- ✅ Can ask "What's the status?" and get accurate info
- ✅ Can ask "Ingest data for YYYY-MM-DD" and it works
- ✅ Can ask "Run scan for YYYY-MM-DD" and it works
- ✅ Error messages are clear and actionable
- ✅ All operations show progress and results

### Phase 2 Complete When:
- ✅ Conversational context maintained
- ✅ Can ask multi-step requests ("Do everything for 2025-02-07")
- ✅ Agent suggests next steps
- ✅ Agent explains failures and fixes

---

## Security Considerations

1. **No Arbitrary Code Execution**
   - Only predefined tool functions
   - No `eval()` or `exec()`
   - No file system writes outside project

2. **Database Safety**
   - Read queries are safe
   - Write operations go through workers
   - No direct SQL execution via chat

3. **User Confirmation**
   - Write operations show intent
   - Can cancel before execution
   - Clear audit trail

4. **Isolation**
   - Agent runs with same permissions as user
   - No privilege escalation
   - Operations logged in job_run table

---

## Dependencies

Add to `pyproject.toml`:

```toml
[project.dependencies]
# ... existing dependencies ...
"phidata>=0.11.0"
"openai>=1.60.0"
```

---

## Next Steps

1. **Create tools.py** with safe command execution functions
2. **Test tools** directly without Phidata
3. **Integrate Phidata** and test agent
4. **Update Chat page** to use agent
5. **End-to-end testing** with real data

---

**Status**: Ready to implement
**Estimated Effort**: 4-6 hours for Phase 1
**Priority**: High (user-requested feature)
