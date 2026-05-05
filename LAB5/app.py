from flask import Flask, request  # Dodano import request

app = Flask(__name__)

@app.route("/")
def home():
    return "Witaj w systemie monitoringu transakcji!"

@app.route("/hello")
def abc():
    # Zmieniono [] na () oraz dodano brakujący cudzysłów w return
    name = request.args.get('name', 'nieznajomy')
    return f"hello {name}"

if __name__ == "__main__":
    app.run(port=8999)
