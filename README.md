# scheduler
Local scheduler for workflow

## Development

if you wanna develop a new middleware, please see [middleware.md](./examples/middleware/middleware.md)

if you wanna develop a new event handler, please see [event.md](./examples/event/event.md)

## Test

use goconvey and forward to a port you can access.

if you wanna see `zap` log, please import `"github.com/tass-io/scheduler/pkg/utils/log"` at the test file

if you wanna run integrity test at the top-level, you should add `-p 1`.

make sure your test machine has nodejs environment for all test run `scheduler/user-code`