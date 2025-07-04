# 👨🏻‍💻 Development Guide

Thanks for your interest in contributing to `SbatchMan`!  
This guide will help you set up a local development environment so you can work on the code and documentation effectively.

## Create a Virtual Environment

It is recommended to use a virtual environment to isolate project dependencies.

```bash
# Create a virtual environment
python3 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
```

## Developer Installation

If you'd like to contribute or simply modify the internal behavior of SbatchMan:

### Clone the GitHub repository

```bash
git clone git@github.com:LorenzoPichetti/SbatchMan.git
```

### Install Locally

From the `SbatchMan` root folder, run: 

```bash
# Install the project in editable mode
pip install -e .
# Install documentation dependencies
pip install -r docs/requirements.txt
```

Now the `sbatchman` command should use the local version of the package. Any changes you make to the code will be reflected immediately when you run the command.

## Run the Documentation Server

To preview the documentation site locally, run the `mkdocs` server.

```bash
mkdocs serve
```

The documentation will be available at `http://127.0.0.1:8000`. The server will automatically reload when you make changes to the documentation files.