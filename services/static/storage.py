from os import listdir
from os.path import isfile, join
from dotenv import load_dotenv
from helpers.log.logger import logger
import os


load_dotenv("config/.env")


class ListExcelFiles:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando ListExcelFiles")

    def _list_files(self):
        path = os.getenv("EXCEL_PATH")
        self.log.info(f"Listando arquivos no diretório: {path}")

        try:
            files = [f for f in listdir(path) if isfile(join(path, f))]
            self.log.info(f"Arquivos encontrados: {len(files)}")
            return files

        except Exception:
            self.log.error("Erro ao listar arquivos no EXCEL_PATH", exc_info=True)
            raise


class UploadFiles:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando UploadFiles")

    def _upload_files(self, file):
        path = os.getenv("EXCEL_PATH")
        target_path = f"{path}/{file.filename}"

        self.log.info(f"Iniciando upload de arquivo: {file.filename}")

        try:
            content = file.file.read()

            with open(target_path, "wb") as f:
                f.write(content)

            self.log.info(f"Upload concluído: {file.filename} ({len(content)} bytes)")
            return {"filename": file.filename, "size": len(content)}

        except Exception:
            self.log.error(f"Erro ao fazer upload do arquivo {file.filename}", exc_info=True)
            raise


class DeleteFiles:
    def __init__(self):
        self.log = logger("static")
        self.log.info("Inicializando DeleteFiles")

    def _delete_files(self, filename):
        path = os.getenv("EXCEL_PATH")
        file_path = os.path.join(path, filename)

        self.log.info(f"Solicitado delete do arquivo: {filename}")

        try:
            os.remove(file_path)
            self.log.info(f"Arquivo removido com sucesso: {filename}")
            return {"message": f"File '{filename}' succesfully removed."}

        except FileNotFoundError:
            self.log.error(f"Arquivo '{filename}' não encontrado para remoção", exc_info=True)
            raise

        except Exception:
            self.log.error(f"Erro ao remover arquivo '{filename}'", exc_info=True)
            raise