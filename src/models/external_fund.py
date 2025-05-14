import pandera.polars as pa
from typing import Optional


class ExternalFund(pa.DataFrameModel):
    """
    DataFrame model for external fund data.
    """

    FUND: str
    DATE: str
    FINANCIAL_TYPE: str
    SYMBOL: str
    SECURITY_NAME: str
    SEDOL: Optional[str] = pa.Field(nullable=True)
    ISIN: Optional[str] = pa.Field(nullable=True)
    PRICE: Optional[float] = pa.Field(nullable=True)
    QUANTITY: Optional[float] = pa.Field(nullable=True)
    REALISED_P_L: Optional[float] = pa.Field(nullable=True)
    MARKET_VALUE: Optional[float] = pa.Field(nullable=True)
