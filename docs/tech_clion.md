# CLion

## Setup

### Configuring CLion

Note that **File** > **Settings** is **CLion** > **Preferences** on macOS.

**Cloning**
1. Launch CLion.
2. **Get from Version Control** > **GitHub** > **Log In via GitHub** > Log in to your GitHub account.
3. Pick `noisepage` from the dropdown list and then click `Clone`. By default, this clones to `~/CLionProjects/noisepage`.
   - Note: if this is your first time installing NoisePage, you will need to install the necessary packages.
   - Go to the folder: `cd ~/CLionProjects/noisepage/script/installation`
   - Install all the necessary packages: `sudo bash ./packages.sh`

**Required: Profiles**
1. In the top left, go to **File** > **Settings** > **Build, Execution, Deployment** > **CMake**.  
2. If you only have a `Debug` profile, hit the `+` button and you should get a `Release` profile.
3. We will figure out how many cores you have.
   - In the terminal, run `python -c 'import multiprocessing as mp; print(mp.cpu_count())'`.
   - Substitute that number for `NOISEPAGE_TEST_PARALLELISM`'s `NUM_CORES` below.
4. For the `Debug` profile, set the following:
   - Build type: Debug
   - CMake options: `-GNinja -DCMAKE_BUILD_TYPE=Debug -DNOISEPAGE_USE_ASAN=ON -DNOISEPAGE_TEST_PARALLELISM=NUM_CORES`
5. For the `Release` profile, set the following:
   - Build type: Release
   - CMake options: `-GNinja -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_TEST_PARALLELISM=NUM_CORES`
6. If you have at least 16 GB of RAM, you can:
   - Significantly improve compilation time: add `-DNOISEPAGE_UNITY_BUILD=ON` to the CMake options of both `Debug` and `Release` above.

**Required: clang-tidy**  
1. In the top left, go to **File** > **Settings** > **Editor** > **Inspections** > **C/C++** > **General** > **Clang-Tidy**.
2. Click the link for `Specify Clang-Tidy executable` and point it to `/bin/clang-tidy-8`. The default CLion-bundled version may provide different warnings.
3. Make sure `Prefer .clang-tidy files over IDE settings` is checked.

**Required: clang-format**  
1. In the top left, go to **File** > **Settings** > **Editor** > **Code Style**.
2. Make sure `Enable ClangFormat` is checked. This will force CLion to use the project's .clang-format file for formatting operations.

### Configuring version control

1. In the top bar, go to **VCS** > **Git** > **Remotes**.
2. Make sure that `origin` corresponds to `git@github.com:YOUR_GITHUB_USERNAME/noisepage.git`.
3. Add a new remote:
   - Name: upstream
   - URL: `git@github.com:cmu-db/noisepage.git` 

### Using CLion

#### Interface

Practically speaking, you need to know about the following 9 parts of the CLion interface:

[![CLion Interface](https://i.imgur.com/A6kvN6j.png)](https://imgur.com/a/MKXu2gv)

1. The Project pane. You can open and close it by clicking on it.
2. The contents of the Project pane. This shows all the files in the project.
3. The Build (hammer) button represents `Build configuration`.
4. A dropdown of configurations, shown as `target | profile`. You can pick between the `Debug` profile and `Release` profile, which you created earlier. You can also pick the target that you want to build. The main [docs README](https://github.com/noisepage/docs/README.md) describes targets in more detail.
5. The Run (play) button represents `Run configuration` and the Debug (looks-like-a-bug) button represents `Debug configuration`.
   - When in doubt, hover over the button and it will say what it does (and give you a keyboard shortcut).
   - The vast majority of your time will be spent running and debugging things.
   - The difference between `building` and `running` is that the former makes the target without actually running it.
   - Some targets are fake and therefore can be built but cannot be run. For example, `check-format` runs a Python script that checks for code style problems in the database. Therefore you use the hammer and not the play button for those.
6. The Messages pane is where most output gets printed. If it would have been printed on the terminal, for example when using `Build configuration`, then it will be printed here instead.
7. The Terminal pane is where you can create terminal instances. Useful.
8. The CMake pane shows you all the CMake output for all your profiles. If you're posting about an issue, this information is usually helpful.
9. The Git Branch indicator shows you what Git branch you are currently on. If you click on it, you can do other things like Merge into current branch from here.

#### Daily life

- Build
    - You should default to the Build button (Ctrl+F9) for all targets.
- Run
    - To run NoisePage, select the `noisepage` build target and use the Run button (Shift+F10).
    - To run a specific unit test, select that unit test from the drop down menu at the top of the IDE and use the Run button.
- Debug
    - Use CLion's [built-in debugger](https://blog.jetbrains.com/clion/2015/05/debug-clion/) to set breakpoints and step through the code.
    - To debug NoisePage, select the `noisepage` build target and use the Debug button (Shift+F9).
    - To debug a specific unit test, select that unit test from the drop down menu at the top of the IDE and use the Debug button.
    - To attach to an existing process, press (Shift+Shift) and type "Attach to Process", then provide the PID. You may need to set `ptrace_scope`, i.e., in a terminal first execute `echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope`.

#### Useful shortcuts to know

Note: If you are on a Mac and these shortcuts do not work, try `Cmd` instead of `Ctrl`. Most shortcuts are from Ubuntu 20.04.

- `Ctrl`+`Shift`+`F`: [find everywhere](https://www.jetbrains.com/help/clion/finding-and-replacing-text-in-project.html) for the provided **string**.
- `Ctrl`+`Shift`+`R`: [replace everywhere](https://www.jetbrains.com/help/clion/finding-and-replacing-text-in-project.html) for the provided **string**.
   - Note that you can replace with matching capitalization and with regex capture groups. Very powerful. 
- `Shift`+`Shift`: [search everywhere](https://www.jetbrains.com/help/clion/searching-everywhere.html) for the provided **symbol**. Symbols are like class names, file names, etc.
- `Ctrl`+`Alt`+`L`: [reformat code](https://www.jetbrains.com/help/idea/reformat-and-rearrange-code.html) in the current file.
- `Ctrl`+`Click`: [quick definition](https://www.jetbrains.com/help/clion/viewing-definition.html), hover over symbol to see documentation, click to jump to definition.
- `Alt`+`Click`: [multiple cursors](https://www.jetbrains.com/help/rider/Multicursor.html).
- `Alt`+`Shift`+`Left`: go to previous cursor location. (like browser history)
- `Alt`+`Shift`+`Right`: go to next cursor location. (like browser history)
- `Ctrl`+`Shift`+`Up/Down`: move source code up/down.
- `Ctrl`+`K`: bring up the [commit](https://www.jetbrains.com/help/clion/commit-and-push-changes.html) window.
   - In this window, you can use `Ctrl`+`D` to quickly view the diff for a changed file.
- [CLion reference card](https://resources.jetbrains.com/storage/products/clion/docs/CLion_ReferenceCard.pdf) has more.
- Found something else that you use almost daily? Add it here!

#### Other info

- Note that [full remote development](https://www.jetbrains.com/help/clion/remote-development.html#full-remote-procedure) is possible but discouraged.