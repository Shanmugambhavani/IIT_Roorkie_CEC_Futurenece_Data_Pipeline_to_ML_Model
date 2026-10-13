# NammaMart Assignment: Setup on a New Laptop

Start to finish. Allow about an hour, most of it downloads running in the background.

Your work so far is already on GitHub at
`github.com/Shanmugambhavani/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model`
with Part 0 committed. Nothing was lost. This guide gets the new machine to the point
where you can run Airflow.

---

## Step 1: Check the machine can actually run Docker

Do this before installing anything. If it fails, the rest is wasted effort.

1. Press Ctrl+Shift+Esc to open Task Manager
2. Performance tab, click CPU
3. Bottom right, look for **Virtualization**

It must say **Enabled**. If it says Disabled, you need to turn on virtualisation in BIOS
(usually called Intel VT-x, AMD-V, or SVM Mode). Restart, press F2 or Del during boot,
find it under CPU or Advanced settings, enable, save and exit.

Also confirm you have **Administrator rights** on this laptop. Right click the Start
button and look for Terminal (Admin) or PowerShell (Admin). If it is greyed out or asks
for a password you do not have, stop here. Everything below needs admin.

---

## Step 2: Install WSL

WSL is the Linux layer Docker needs on Windows. Install it first, because it needs a
restart and you can do the other downloads while it works.

Open **PowerShell as Administrator**: Windows key, type `powershell`, right click
Windows PowerShell, Run as administrator.

```powershell
wsl --install
```

Then **restart Windows**. Not optional.

After the restart a window may ask you to create a Linux username and password. You can
close it, Docker does not need it. If you do create one, write it down.

Verify:

```powershell
wsl --status
```

You want it to report a default distribution and WSL version 2.

---

## Step 3: Install the five tools

Install in this order. The first three are quick.

| # | Tool | Where | Notes |
|---|---|---|---|
| 1 | Python 3.12 | python.org/downloads | **Tick "Add Python to PATH"** on the first installer screen. Easy to miss, breaks everything if skipped |
| 2 | Git | git-scm.com/download/win | Accept all defaults |
| 3 | GitHub Desktop | desktop.github.com | Sign in with the account that owns the fork |
| 4 | Docker Desktop | docker.com/products/docker-desktop | Keep "Use WSL 2" ticked. Restart when asked |
| 5 | VS Code | code.visualstudio.com | Optional but you will want it for editing DAGs |

On Python: prefer 3.12 over the newest release. Airflow and its ecosystem lag behind the
latest Python by a year or so, and 3.12 avoids a class of package problems you do not
need right now.

Then install the Astro CLI. PowerShell as Administrator:

```powershell
winget install -e --id Astronomer.Astro
```

Close and reopen the terminal afterwards so PATH updates.

---

## Step 4: Verify everything before going further

Open a **new** Command Prompt and run these one at a time:

```
python --version
git --version
docker ps
astro version
```

| Command | What good looks like |
|---|---|
| `python --version` | `Python 3.12.x` |
| `git --version` | `git version 2.x.x` |
| `docker ps` | An empty table with CONTAINER ID, IMAGE, COMMAND headers |
| `astro version` | `Astro CLI Version: 1.x.x` |

For `docker ps`, Docker Desktop must be open **and** showing **Engine running** with a
green dot, bottom left of its window. The window appearing is not the same as the engine
being ready, it takes 30 to 60 seconds.

If any of the four fails, fix it before continuing. Moving on with a broken tool wastes
more time than fixing it now.

---

## Step 5: Clone your repo

Open GitHub Desktop. File, Clone repository, GitHub.com tab. Find
`IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model`.

**Local path matters.** Use something short with no spaces and no cloud sync:

```
C:\dev\fde
```

Not Documents. Not OneDrive. Not Desktop if Desktop is synced. Docker mounts this folder
into containers, and sync clients lock files mid-write, producing failures that look like
Airflow bugs and are not.

If the repo does not appear in the list, use the URL tab and paste:

```
https://github.com/Shanmugambhavani/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model
```

---

## Step 6: Confirm Part 0 came through

Open a terminal in the cloned folder. In GitHub Desktop: Repository menu, Open in
Command Prompt.

```
dir
```

You should see `dags`, `include`, `assignment`, `tests`, `Dockerfile`, `README.md`,
`SUBMISSION.md`.

```
dir include\data\nammamart
dir include\profiling
```

The first should list seven files, the six CSVs plus a README. The second should show
`profile_nammamart.py` and an `out` folder.

If both are there, Part 0 transferred intact and you can skip the zip entirely.

If `include\data\nammamart` is missing, the dataset copy was never committed. Fix it:

```
mkdir include\data\nammamart 2>nul & copy assignment\nammamart_dataset\* include\data\nammamart\
```

---

## Step 7: Re-run the profiler

```
pip install pandas
python include/profiling/profile_nammamart.py --data include/data/nammamart --out include/profiling/out
```

Expected output:

```
Profiled 6 files into include/profiling/out/profile_summary.txt
34 issue groups, 550 offending rows
```

If you get those numbers, the data is in the right place and Python works. That is the
whole point of this step.

---

## Step 8: Start Airflow

Docker Desktop open, Engine running, green dot. Then:

```
astro dev start
```

The first run downloads the Astro Runtime image. Five to ten minutes, with long silent
stretches. Leave it alone.

Success:

```
Project image has been updated
Project started
Airflow UI: http://localhost:8080
```

A browser opens. Log in with `admin` / `admin`.

You should see `meridian_pipeline` and `exampledag` in the DAG list, with no red import
error banner at the top.

Confirm the dataset is visible from inside the container:

```
astro dev bash
ls include/data/nammamart
exit
```

Seven files listed means the mount is working and your DAGs will be able to read them.

---

## Commands you will use daily

| Command | When |
|---|---|
| `astro dev start` | Start Airflow |
| `astro dev stop` | End of session, frees RAM |
| `astro dev restart` | After editing `requirements.txt` |
| `astro dev run dags list-import-errors` | Fastest way to catch a syntax error in a DAG |
| `astro dev run dags test <dag_id>` | Run a DAG once from the terminal, full logs in the foreground |
| `astro dev bash` | Shell inside the scheduler container |
| `astro dev kill` | When something is stuck and nothing else works |

---

## Two things that will trip you up later

**Container paths, not Windows paths.** Inside Airflow, your CSVs are at
`/usr/local/airflow/include/data/nammamart/orders.csv`, never `C:\dev\fde\...`. The
trainer's `meridian_pipeline.py` hardcodes the container path for exactly this reason.
Copy that pattern. Only `include/` is mounted, which is why the dataset had to be copied
out of `assignment/`.

**DuckDB holds a file lock.** Close any VS Code SQLTools connection to the warehouse
before triggering a DAG run, or the task fails with a lock error.

---

## If Docker is blocked on this laptop too

Stop and say so before spending more time. There are fallbacks worth discussing, a cloud
VM or a colleague's machine, but all of them change the plan and are better decided early
than at the deadline.

55 of the 100 marks sit behind having Airflow running: both pipelines, the schedule
screenshots, and the ten observability experiments.
