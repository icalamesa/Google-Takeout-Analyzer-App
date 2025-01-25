from flask import Flask, render_template, request, jsonify
from dash_app import init_dash_app
from data_interface import GoogleTakeoutProcessor 
import os

# Initialize the GoogleTakeoutProcessor
takeout_processor = GoogleTakeoutProcessor(
    takeout_path=os.environ.get('TAKEOUT_PATH', '/home/ivan/Desktop/datasets/other_takeouts/Takeout'),
    data_output_folder='data',
    reset_db=True
)

def create_app():
    """Create and configure the Flask app."""
    server = Flask(__name__)

    @server.route('/')
    def index():
        """Render the landing page."""
        return render_template('landing.html')
    
    @server.route('/dashboard')
    def dashboard():
        """Render the dashboard page."""
        return render_template('dashboard.html')
    
    @server.route('/query_dashboard')
    def query_dashboard():
        """Render the query dashboard page."""
        return render_template('queries.html')

    @server.route('/api/run-query', methods=['POST'])
    def run_query():
        """
        API endpoint to execute a SQL query using the TakeoutProcessor.
        Returns the results as JSON.
        """
        try:
            data = request.get_json(force=True)
            sql_query = data.get('sql')
            if not sql_query:
                return jsonify({"error": "No SQL query provided"}), 400

            result_df = takeout_processor.query_data(sql_query)
            result_data = result_df.to_dict(orient='records')
            
            return jsonify({"data": result_data})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    dash_app = init_dash_app(server, pathname='/dash/', takeout_processor=takeout_processor)
    return server

if __name__ == '__main__':
    app = create_app()
    app.run(port=5000, debug=True)
