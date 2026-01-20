#!/bin/bash

# Переход в директорию скрипта (на всякий случай)
cd "$(dirname "$0")"

# Активация виртуального окружения
source ./venv/bin/activate

# Запуск бота
python3 main.py