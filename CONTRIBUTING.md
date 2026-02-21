# Contributing to MarketMind

First off, thank you for considering contributing to MarketMind. We value our community and welcome any contributions, whether they be bug reports, feature requests, or code contributions!

## How to Contribute

### 1. Reporting Bugs
- Ensure the bug was not already reported by searching on GitHub under [Issues](https://github.com/your-org/MarketMind/issues).
- If you're unable to find an open issue addressing the problem, open a new one. Be sure to include a title and clear description, as much relevant information as possible, and a code sample or an executable test case demonstrating the expected behavior that is not occurring.

### 2. Suggesting Enhancements
- Open a new enhancement issue. Provide a clear and descriptive title.
- Explain why this enhancement would be useful to most MarketMind users.

### 3. Pull Requests
The process described here has several goals:
- Maintain MarketMind's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible MarketMind
- Enable a sustainable system for MarketMind's maintainers

#### PR Process
1. Fork the repo and create your branch from `main`.
2. Ensure you have the [prerequisites](../README.md#prerequisites) set up.
3. If you've added code that should be tested, add tests.
4. Update the documentation, if necessary.
5. Ensure the test suite passes.
6. Make sure your code lints (run `ruff check .` / `black .`).
7. Issue that pull request!

## Local Development Environment

MarketMind is composed of multiple services (PostgreSQL, Redis, Ollama, API, FastAPI workers, and a Next.js frontend). The easiest way to get everything running is via Docker Compose.

```bash
docker-compose --profile full up -d
```

### Python Backend (API & Workers)
- We use Python 3.10+.
- All dependencies are managed in `pyproject.toml`.
- Use an isolated environment (like `venv` or `conda`).
- To run linting: `ruff check core/ api/ workers/`
- To format: `black core/ api/ workers/`

### Node.js Frontend
- Located in `interface/dashboard/`.
- Ensure you have a recent version of Node.js installed.
- Install dependencies with `npm install` or `yarn install`.
- Run formatting and linting: `npm run lint`

## Branching Guidelines
- `main` is our production-ready branch.
- Prefix your branches accordingly:
   - `feat/`: A new feature
   - `fix/`: A bug fix
   - `docs/`: Documentation changes
   - `chore/`: Maintenance, refactoring, dependencies
   - `test/`: Adding missing tests

Thank you for your contributions!
