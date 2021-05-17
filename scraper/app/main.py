"""
Minimal FastAPI application taken directly from the tutorial.
https://fastapi.tiangolo.com/
"""

from fastapi import FastAPI
from pydantic import BaseModel

from scraper import rfaster

app = FastAPI()


class Item(BaseModel):
    name: str
    price: float
    is_offer: bool = None


@app.get("/")
def read_root():
    return {"Hello": "scraper test"}


@app.get("/rentfaster/listings/{city_id}/{page}")
def get_rentfaster_listings(city_id: int = 1, page: int = 1):
    return rfaster.get_listings(city_id, page)


@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}
