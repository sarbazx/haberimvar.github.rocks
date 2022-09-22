from flask import Flask, request, jsonify
from flask import Response
import final_nlp_model
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    new_title = "How to Watch Apple’s iPhone 14 Launch"
    data = final_nlp_model.news_recommender(new_title)
    return Response(data, mimetype='application/json')

@app.route('/recommended', methods = ['POST'])
def recomended():
    content = request.json
    title = content['title']
    data = final_nlp_model.news_recommender(title)
    response = Response(data, mimetype='application/json')
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/<news_id>')
def news(news_id=0):
    data = final_nlp_model.get_news_by_id(str(news_id))
    response = Response(data, mimetype='application/json')
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/initial')
def initial():
    data = final_nlp_model.get_initial_data()
    response = Response(data, mimetype='application/json')
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
