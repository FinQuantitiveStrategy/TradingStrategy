# flask imports
from flask import Flask
#from flask import request
from flask import send_from_directory
from flask import redirect
from flask import request
from flask import Response

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

## Backtesting
# Fire backtest
bttask = None
@app.route('/api/backtest-fire', methods=['GET'])
def backtestfire():
    global bttask
    bttask = threading.Thread(target=Schedule.backtestToSelect)
    bttask.start()
    return "Backtest Fired"
# Check backtest status
@app.route('/api/backtestchk', methods=['GET'])
def backtestcheck():
    if(bttask == None):
        return 'Task not created'
    if(bttask.is_alive() == True):
        return 'Task running'
    else:
        return 'Previous Task Finished'
# Read backtest data
@app.route('/api/backtestresult', methods=['GET'])
def getresult():
    if Schedule.testdat == {}:
        return 'Empty data.'
    category = request.args.get('type')
    taskname = request.args.get('name')
    if category == 'csv':
        if taskname in Schedule.testdat['txa'].keys():
            return Response(Schedule.testdat['txa'][taskname], mimetype='text/plain')
    elif category == 'log':
        if taskname in Schedule.testdat['log'].keys():
            return Response(Schedule.testdat['log'][taskname], mimetype='text/plain')
    
    return 'Parameter Error.'

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