// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup

import net.diet_rich.util.data.Bytes

case class TimeSizePrintHash(time:  Long, size:  Long, print:  Long, hash: Bytes)
