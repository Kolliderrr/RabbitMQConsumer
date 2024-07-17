"""_summary_


"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

from flask_main import create_app as flask_app
# from dash_applet.dash_app import create_dash
from consumers.consumer_app import app as api_app


flask_app = flask_app()
# socketapi_app = socket_app

main_app = FastAPI()

# Монтируем Flask приложение
main_app.mount("/flask", WSGIMiddleware(flask_app))

# Добавляем маршруты FastAPI
main_app.include_router(api_app.router)

if __name__ == "__main__":
    uvicorn.run("server:main_app", host="127.0.0.1", port=8008, log_level="info", reload=True)
