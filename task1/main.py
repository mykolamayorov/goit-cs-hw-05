import argparse
import asyncio
import logging
from pathlib import Path
import shutil
from aiofiles.os import wrap

# Асинхронна обгортка для shutil.copy
async_copy = wrap(shutil.copy)

# Налаштування логування помилок
logging.basicConfig(
    filename='file_sorter.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def copy_file(file_path: Path, output_folder: Path):
    """Копіює файл у відповідну підпапку за розширенням."""
    try:
        ext = file_path.suffix.lower().lstrip('.') or 'no_extension'
        target_dir = output_folder / ext
        target_dir.mkdir(parents=True, exist_ok=True)
        await async_copy(str(file_path), str(target_dir / file_path.name))
        print(f"Copied {file_path} -> {target_dir}")
    except Exception as e:
        logging.error(f"Error copying {file_path}: {e}")


async def read_folder(source_folder: Path, output_folder: Path):
    """Рекурсивно читає всі файли та копіює їх асинхронно."""
    tasks = []
    for file_path in source_folder.rglob('*'):
        if file_path.is_file():
            tasks.append(copy_file(file_path, output_folder))
    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Асинхронний сортер файлів за розширенням")
    parser.add_argument("source", type=str, help="Шлях до вихідної папки")
    parser.add_argument("output", type=str, help="Шлях до цільової папки")
    args = parser.parse_args()

    source_folder = Path(args.source)
    output_folder = Path(args.output)

    if not source_folder.exists():
        print(f"Вихідна папка {source_folder} не існує!")
        return

    asyncio.run(read_folder(source_folder, output_folder))
    print("Сортування завершено!")


if __name__ == "__main__":
    main()