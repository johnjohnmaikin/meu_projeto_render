from flask import Flask, jsonify
import pandas as pd
import requests

app = Flask(__name__)


def buscar_dados():
    link = "8101188074:AAHkS6A28wGm7JAkXuUBOlogi5k7x3w5fno"

    html = requests.get(link, timeout=20).text
    df = pd.read_html(html)[0]

    # Exemplo: transforma o DataFrame em lista de dicionários
    resultado = df.fillna("").to_dict(orient="records")

    return resultado


@app.route("/")
def home():
    return "Aplicação rodando no Render!"


@app.route("/dados")
def dados():
    try:
        resultado = buscar_dados()
        return jsonify(resultado)
    except Exception as erro:
        return jsonify({"erro": str(erro)}), 500
if __name__ == "__main__":
    app.run(debug=True)