#!/bin/bash

# This file is part of VertNet: https://github.com/VertNet/webapp
#
# VertNet is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VertNet is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see: http://www.gnu.org/licenses

# This script compiles JavaScript in www/ into www-built/ and then deploys the 
# entire app to App Engine. 
# Example usage: $ ./deploy.js <app_engine_password> <app_version>
node r.js -o build.js
echo $1 | appcfg.py update --email=eightysteele@gmail.com --passin -V $2 ../
