#!/bin/bash

# Root folder
mkdir -p data-cleaner
cd data-cleaner || exit

# Frontend
mkdir -p frontend
touch frontend/index.html
touch frontend/style.css
touch frontend/app.js
touch frontend/README.md

# Backend
mkdir -p backend/app

# Backend core structure
mkdir -p backend/app/api
mkdir -p backend/app/core
mkdir -p backend/app/services
mkdir -p backend/app/models
mkdir -p backend/app/utils
mkdir -p backend/app/temp/uploads
mkdir -p backend/app/tests

# Backend files
touch backend/app/main.py

touch backend/app/api/routes.py

touch backend/app/core/config.py
touch backend/app/core/logger.py

touch backend/app/services/loader.py
touch backend/app/services/profiler.py
touch backend/app/services/cleaner.py
touch backend/app/services/type_fixer.py
touch backend/app/services/validator.py
touch backend/app/services/reporter.py

touch backend/app/models/report.py

touch backend/app/utils/file_utils.py
touch backend/app/utils/constants.py

touch backend/app/tests/test_loader.py
touch backend/app/tests/test_cleaner.py
touch backend/app/tests/test_validator.py

touch backend/requirements.txt
touch backend/README.md

# Docs
mkdir -p docs
touch docs/pipeline.md
touch docs/architecture.md
touch docs/api.md

# Root files
touch README.md
touch .gitignore
touch LICENSE

echo "✅ MVP folder structure created successfully."
