"""
** código responsável por verificar se existe algo na LT22 ou não
1. se houver, extrair .txt da lt22
2. se não houver, apenas sair da LT22
"""
from dotenv import load_dotenv
import os


load_dotenv("config/.env")

def full_lt22_path():
    base = os.environ["USERPROFILE"]
    return os.path.join(base, ".000 - Projetos", "auto-line-feeding", "backend", "core", "storage", "sap")


def lt22_has_data(session):
    return extract_lt22(session) if session.FindById("wnd[0]/usr/lbl[4,5]") else False

def extract_lt22(session):
    # print(os.getenv("SAP_PATH_STORAGE"))
    session.findById("wnd[0]/tbar[1]/btn[9]").press()
    session.findById("wnd[1]/usr/subSUBSCREEN_STEPLOOP:SAPLSPO5:0150/sub:SAPLSPO5:0150/radSPOPLI-SELFLAG[1,0]").select()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = full_lt22_path()

    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "lt22.txt"
    session.findById("wnd[0]/tbar[1]/btn[9]").press()
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    
    # session.findById("wnd[3]/usr/ctxtDY_FILENAME").setFocus()
    # session.findById("wnd[3]/usr/ctxtDY_FILENAME").caretPosition = 8
    # session.findById("wnd[3]/tbar[0]/btn[0]").press()
    # session.findById("wnd[2]").sendVKey(12)
    # session.findById("wnd[1]").sendVKey(12)
