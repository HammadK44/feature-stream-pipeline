import typer
import uvicorn
from feat_stream.pipeline import run_forever, run_once

app = typer.Typer(no_args_is_help=True)

@app.command()
def run():
    result = run_once()
    typer.echo(result)

@app.command()
def loop():
    run_forever()

@app.command()
def serve(host: str = '0.0.0.0', port: int = 8000):
    uvicorn.run('feat_stream.api.main:app', host=host, port=port)
