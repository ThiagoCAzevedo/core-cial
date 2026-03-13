from modules.requests_builder.domain.models import RequestsMade
from sqlalchemy import select
from sqlalchemy.orm import Session
from common.logger import logger
from sqlalchemy import update
import polars as pl, re


class LM01RequesterService:
    def __init__(self, sap, db: Session):
        self.sap = sap
        self.db = db
        self.log = logger("requests_builder")
        self.log.info("Initializing LM01RequesterService")
        self._load_requests_data()

    def _load_requests_data(self):
        try:
            stmt = select(
                RequestsMade.partnumber,
                RequestsMade.num_reg_circ,
                RequestsMade.qty_to_request,
                RequestsMade.qty_boxes_to_request,
                RequestsMade.takt,
                RequestsMade.rack,
                RequestsMade.num_shipment,
            )

            result = self.db.execute(stmt)
            rows = [tuple(r) for r in result]
            columns = result.keys()
            self.df = pl.DataFrame(rows, schema=columns)

            self.log.info(f"Loaded {len(rows)} records for SAP LM01")
        except Exception:
            self.log.error("Error loading requests data", exc_info=True)
            raise

    def request_lm01(self) -> int:
        self.log.info("Starting SAP LM01 request")

        if not self.sap:
            raise Exception("No SAP session available")

        try:
            session, _ = self.sap.run_transaction("/nLM01")
            session.findById("wnd[0]/usr/btnRLMOB-PBACK").press()
            session, _ = self.sap.run_transaction("/nLM01")
            self.log.info("SAP LM01 session OK")
        except Exception:
            self.log.error("Error starting SAP LM01", exc_info=True)
            raise

        try:
            session.findById("wnd[0]/usr/btnTEXT2").press()
            session.findById("wnd[0]/usr/btnTEXT1").press()
        except Exception:
            self.log.error("Error preparing LM01 UI", exc_info=True)
            raise

        rows_requested = 0

        try:
            for row in self.df.iter_rows(named=True):
                qtd_caixas = int(row["qty_boxes_to_request"])
                num_circ = str(row["num_reg_circ"])

                self.log.info(f"Processing part={row['partnumber']} | boxes={qtd_caixas}")

                for _ in range(qtd_caixas):
                    rows_requested += 1

                    try:
                        session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                        session.findById("wnd[0]").sendVKey(0)
                        session.findById("wnd[0]").sendVKey(0)
                        session.findById("wnd[0]").sendVKey(8)

                        num_shipment = self._get_shipment_number(session)
                        if num_shipment:
                            self._update_shipment_number(num_circ, num_shipment)
                        else:
                            self.log.warning(f"No OT returned for {num_circ}")

                        session.findById("wnd[0]").sendVKey(0)

                    except Exception:
                        self.log.error(f"Error requesting for num_circ={num_circ}", exc_info=True)
                        raise

            self.log.info(f"LM01 completed — total boxes: {rows_requested}")

        except Exception:
            self.log.error("Error in LM01 loop", exc_info=True)
            raise

        return rows_requested

    def _get_shipment_number(self, session) -> str | None:
        """Extract order number from SAP LM01 response"""
        try:
            msg = session.findById("wnd[0]/usr/txtGV_300_MSG2").Text
            match = re.search(r"(\d+)", msg)

            if match:
                ot = match.group(1)
                self.log.info(f"OT captured: {ot}")
                return ot

            self.log.warning(f"No OT found in message: {msg}")
            return None

        except Exception:
            self.log.error("Error capturing OT", exc_info=True)
            return None

    def _update_shipment_number(self, num_circ: str, num_shipment: str):
        try:
            self.log.info(f"Updating shipment number in DB: {num_circ} → {num_shipment}")

            stmt = (
                update(RequestsMade)
                .where(RequestsMade.num_reg_circ == num_circ)
                .values(num_shipment=num_shipment)
            )

            result = self.db.execute(stmt)
            self.db.commit()

            if result.rowcount == 0:
                self.log.warning(f"No row found to update (num_reg_circ={num_circ})")

        except Exception:
            self.log.error(f"Error updating shipment number for {num_circ}", exc_info=True)
            raise