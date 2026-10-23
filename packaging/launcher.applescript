on run
    set applicationPath to POSIX path of (path to me)
    if applicationPath ends with "/" then set applicationPath to text 1 thru -2 of applicationPath
    set projectPath to do shell script "/usr/bin/dirname " & quoted form of applicationPath
    set launchPath to projectPath & "/launch.py"
    do shell script "/usr/bin/python3 " & quoted form of launchPath & " >/dev/null 2>&1 &"
end run
