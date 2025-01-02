# flask imports
from flask import Flask
#from flask import request
from flask import send_from_directory
from flask import redirect

# functionality imports
import threading
import Schedule

app = Flask(__name__, static_url_path='/static', static_folder='./static')

### Initialization and Constants
scheduler = Schedule.autoUpdate()

### API Functionality
# API Root
@app.route('/api/', methods=['GET']) 
def apiroot():
	return "API Root"

# Fire backtest
@app.route('/api/backtest-fire', methods=['GET'])
def backtestfire():
	threading.Thread(target=Schedule.backtestToSelect).start()
	return "Backtest Fired"

# Trigger data update
@app.route('/api/manual-update', methods=['GET'])
def trigupdate():
	threading.Thread(target=Schedule.dataUpdater).start()
	return "Manual Update Fired"

# Check on Scheduler Status
@app.route('/api/scheduler-running', methods=['GET'])
def schedulerStatus():
	return str(scheduler.running)



### Serve frontend and static files
@app.route('/')
def send_root():
	return redirect('index.html')

@app.route('/<path:path>')
def serve_root(path):
	return send_from_directory('webroot', path)


if __name__ == '__main__':
	app.run(host='0.0.0.0')