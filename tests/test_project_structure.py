from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_required_directories_exist():
    required_directories = [
        "data",
        "data/raw",
        "data/processed",
        "scripts",
        "src",
        "tests",
        "docker",
        "dags",
    ]

    for directory in required_directories:
        assert (PROJECT_ROOT / directory).is_dir(), (
            f"Missing directory: {directory}"
        )


def test_required_files_exist():
    required_files = [
        "requirements.txt",
        ".env.example",
        "README.md",
        "docker/docker-compose.yml",
        "dags/retaillake_pipeline.py",
    ]

    for file in required_files:
        assert (PROJECT_ROOT / file).is_file(), (
            f"Missing file: {file}"
        )