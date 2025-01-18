from flask import Flask, render_template, request, jsonify
from dash_app import init_dash_app
import data_interface
import os

TakeoutDatabase = data_interface.GoogleTakeoutDataInterface(
    takeout_path=os.environ.get('TAKEOUT_PATH', '/home/ivan/Desktop/datasets/Takeout'),
    data_output_folder='data',
    reset_db=True
)

def create_app():
    server = Flask(__name__)

    @server.route('/')
    def index():
        return render_template('landing.html')
    
    @server.route('/dashboard')
    def dashboard():
        return render_template('dashboard.html')
    
    @server.route('/query_dashboard')
    def query_dashboard():
        return render_template('queries.html')

    @server.route('/api/run-query', methods=['POST'])
    def run_query():
        try:
            data = request.get_json(force=True)
            sql_query = data.get('sql')
            if not sql_query:
                return jsonify({"error": "No SQL query provided"}), 400

            result_df = TakeoutDatabase.query_data(sql_query)
            result_data = result_df.to_dict(orient='records')
            
            return jsonify({"data": result_data})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    dash_app = init_dash_app(server, pathname='/dash/')
    return server

if __name__ == '__main__':
    app = create_app()
    app.run(port=5000, debug=True)
