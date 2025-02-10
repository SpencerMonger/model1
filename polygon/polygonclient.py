"""The basic polygon client utilizes a few key methods to interact with Poylgon REST API."""

import os
from typing import Any

import aiohttp
from pydantic import BaseModel, Field

from config import base_settings

from schema import AggregateBar


BASE_URL = os.environ.get("POLYGON_API_URL", None)
API_KEY = os.environ.get("POLYGON_API_KEY", None)


class PolygonClient(BaseModel):
    """The MarketRest class defines the client for the Polygon REST API.

    Attributes
    ----------
        client (RESTClient): The client for the Polygon REST API.

    """

    base_url: str = Field(
        base_settings.POLYGON_API_URL,
        description="The base URL for the Polygon REST API.",
    )
    api_key: str = Field(
        base_settings.POLYGON_API_KEY,
        description="The API key for the Polygon REST API.",
    )

    async def get_daily_aggs(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        adjusted: bool = True,
    ) -> list[AggregateBar]:
        """Get daily aggregated data for a given ticker.

        Parameters
        ----------
        ticker : str
            The stock ticker symbol (e.g. 'SPY', 'AAPL')
        start_date : str
            The start date in format YYYY-MM-DD
        end_date : str
            The end date in format YYYY-MM-DD
        adjusted : bool, optional
            Whether to get adjusted data, by default True

        Returns
        -------
        dict[str, Any]
            The response data from Polygon API

        Raises
        ------
        aiohttp.ClientError
            If there is an error with the request
        """
        endpoint = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": -2,  # No limit
            "apiKey": self.api_key,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint, params=params) as response:
                response.raise_for_status()
                results = await response.json()

        return [AggregateBar(**result) for result in results["results"]]