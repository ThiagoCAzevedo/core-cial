from fastapi import APIRouter, File, UploadFile
from services.static.storage import ListExcelFiles, UploadFiles, DeleteFiles
from helpers.log.logger import logger
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()
log = logger("static")


@router.get("/list", summary="Get files in excel folder")
def list_files():
    log.info("GET /files/list — listed excel files")

    try:
        files = ListExcelFiles()._list_files()
        log.info(f"Finished listing — amount of registers: {len(files)}")
        return files

    except Exception as e:
        log.error("Error listing excel files", exc_info=True)
        raise HTTP_Exceptions().http_500("Error listing excel files: ", e)


@router.post("/upload", summary="Upload file in excel folder")
def upload_files(file: UploadFile = File(...)):
    log.info(f"POST /files/upload — received file: {file.filename}")

    try:
        result = UploadFiles()._upload_files(file)
        log.info(f"Finished file upload: {file.filename}")
        return result

    except Exception as e:
        log.error(f"Error uploading file: {file.filename}", exc_info=True)
        raise HTTP_Exceptions().http_500("Error uploading file: ", e)


@router.delete("/delete/{filename}", summary="Delete file in excel folder")
def delete_files(filename: str):
    log.info(f"DELETE /files/delete — file to remove: {filename}")

    try:
        result = DeleteFiles()._delete_files(filename)
        log.info(f"Successfully removed file: {filename}")
        return result

    except Exception as e:
        log.error(f"Error removing file: {filename}", exc_info=True)
        raise HTTP_Exceptions().http_500("Error removing file: ", e)