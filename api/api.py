#!/usr/bin/env python
import json
import logging

from flask import Flask, request, Response, jsonify

from core import insert_page_view_event, get_behavioral_profile


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)


def _get_error_response(message, status=400):
    return Response(json.dumps({'error': message}),
            mimetype='application/json',
            status=status)

@app.route('/v1/page', methods=['POST'])
def page_view_event():
    try:
        data = request.json
    except Exception, e:
        return _get_error_response(
                'failed to parse payload:\n%s' % request.data)
    try:
        event_id = insert_page_view_event(**data)
    except Exception, e:
        return _get_error_response(str(e))
    return jsonify({'event_id': str(event_id)})

@app.route('/v1/user/<user_id>', methods=['GET'])
def behavioral_profile(user_id):
    try:
        res = get_behavioral_profile(user_id)
    except Exception, e:
        return _get_error_response(str(e))
    return jsonify(res)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
