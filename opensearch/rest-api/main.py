from fastapi import FastAPI
from handler.event_handler import startup, shutdown
import asyncio
from routers.search_router import router as search_router
from routers.deltas_router import router as deltas_router
from routers.demo_router import router as demo_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    # Можно указать конкретные источники, например ["https://example.com"]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все методы HTTP
    allow_headers=["*"],  # Разрешить все заголовки
)
app.include_router(search_router, tags=["Search"], prefix="/search-api/search")
app.include_router(deltas_router, tags=["Deltas"], prefix="/search-api/deltas")
app.include_router(demo_router, tags=["Demo"], prefix="/search-api/demo")
app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)
