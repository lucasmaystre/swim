#!/usr/bin/env python
import time
import random

from flask import Flask, redirect, abort, url_for


app = Flask(__name__)

TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <title>{title}</title>
</head>
<body>
  <h1>{title}</h1>
  <div>
    {body}
  </div>
</body>
</html>
"""


def gen_page(title, links):
    items = ('<li><a href="{}">link {}</a></li>'.format(link, i)
             for i, link in enumerate(links, start=1))
    body = "<ul>{}</ul>".format("\n".join(items))
    return TEMPLATE.format(title=title, body=body)


@app.before_request
def delay():
    time.sleep(random.uniform(2, 4))


@app.route('/')
def homepage():
    links = ["/product/{}".format(i) for i in (1, 2, 3)]
    links.append("/404")
    links.append("/500")
    links.append("/redirect")
    links.append("htt://google.com/#frag")
    links.append("http://domain-that-does-not.exist")
    return gen_page("Homepage", links)


@app.route("/product/<int:pid>")
def product_page(pid):
    links = ["/user/{}".format(i) for i in (1, 2, 3) if i != pid]
    links.append("/product/{}".format((pid % 3) + 1))
    return gen_page("Product {}".format(pid), links)


@app.route("/user/<int:uid>")
def user_page(uid):
    links = ["/product/{}".format(uid)]
    return gen_page("User {}".format(uid), links)


@app.route("/404")
def four_oh_four():
    abort(404)


@app.route("/500")
def five_hundred():
    abort(500)


@app.route("/redirect")
def redirect_page():
    return redirect(url_for('user_page', uid=3))


if __name__ == '__main__':
    app.run(debug=True)
