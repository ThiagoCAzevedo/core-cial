from fastapi import APIRouter, File, UploadFile
from services.static.storage import ListExcelFiles, UploadFiles, DeleteFiles
from helpers.log.logger import logger
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()
log = logger("static")


@router.get("/list", summary="Get Files In Excel Folder")
def list_files():
    log.info("Rota /list chamada — iniciando listagem de arquivos Excel")

    try:
        files = ListExcelFiles()._list_files()
        log.info(f"Listagem concluída — total de arquivos: {len(files)}")
        return files

    except Exception as e:
        log.error("Erro ao listar arquivos na pasta Excel", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao listar arquivos da pasta Excel: ", e)


@router.post("/upload", summary="Upload File in Excel Folder")
def upload_files(file: UploadFile = File(...)):
    log.info(f"Rota /upload chamada — arquivo recebido: {file.filename}")

    try:
        result = UploadFiles()._upload_files(file)
        log.info(f"Upload concluído — arquivo salvo: {file.filename}")
        return result

    except Exception as e:
        log.error(f"Erro ao fazer upload do arquivo: {file.filename}", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro no upload do arquivo: ", e)


@router.delete("/delete/{filename}", summary="Delete File in Excel Folder")
def delete_files(filename: str):
    log.info(f"Rota /delete chamada — arquivo alvo: {filename}")

    try:
        result = DeleteFiles()._delete_files(filename)
        log.info(f"Arquivo removido com sucesso: {filename}")
        return result

    except Exception as e:
        log.error(f"Erro ao remover arquivo: {filename}", exc_info=True)
        raise HTTP_Exceptions().http_500("Erro ao remover arquivo: ", e)