#!/usr/bin/env python3
import requests, hashlib, os, tempfile, io
from time import time, sleep
from tqdm import tqdm, trange
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
from datetime import datetime
from README_TEMPLATE import README_TEMPLATE
from subprocess import check_call

def fetch(url): 
    acc = 0
    while True:
        try:
            print(f"fetching '{url}'")
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                dat = response.content
            print("comparing hashes")
            sig = hashlib.md5()
            for line in response.iter_lines():
                sig.update(line)
            digest = sig.hexdigest()
            fp = os.path.join(tempfile.gettempdir(), hashlib.md5(digest.encode('utf-8')).hexdigest())
            if os.path.isfile(fp) and os.stat(fp).st_size > 0:
                print("no update available")
                acc = 0
                return False
            else:
                print(f"writing to '{fp}'")
                with open(f"{fp}.tmp", 'wb') as f:
                    f.write(dat)
                os.rename(f"{fp}.tmp", fp)
                return dat
        except Exception as error:
            acc += 1
            retry(acc, error)

def timeout(sec):
    for s in (t := trange(sec, ncols=103, leave=False, ascii=' #')):
        t.set_description(uptime())
        sleep(1)

def uptime(): 
    ct = time()
    et = ct - st
    d = (et // 86400) % 365
    h = (et // 3600) % 24
    m = (et // 60) % 60
    s = et % 60
    d, h, m, s = [int(i) for i in (d, h, m, s)]
    d = str(d).zfill(3)
    h, m, s = [str(i).zfill(2) for i in (h, m, s)]
    uptime = f"uptime: {d} {h}:{m}:{s}"
    return uptime

def retry(acc, error):
    print(f"\n{str(acc).zfill(2)}/10: {error}\n")
    if acc < 10:
        timeout(6)
    else:
        print("max retries exceeded")
        exit(1)

def get_array(df, col, dtype):
    return np.array(df[col], dtype=dtype)

def get_arrays(df, dictionary):
    return [get_array(df, col, dtype) for col, dtype in dictionary.items()]

def get_diff(arr):
    arr = np.asarray(arr)
    diff = np.diff(arr)
    arr = np.insert(diff, 0, arr[0])
    arr[arr < 0] = 0
    return arr

def get_diffs(*arrs):
    return [get_diff(arr) for arr in arrs]

def write_csv(dictionary, fn):
    pd.DataFrame(dictionary).to_csv(fn, index=False)

def plot(arrays, suffix, fp):
    fig, ax = plt.subplots(2, 2, figsize=(12, 7), dpi=200)
    fig.suptitle(f"{suffix} COVID-19 Data")
    x = dates
    fig.autofmt_xdate()
    if total_cases[-1] >= 1_000_000:
        y = total_cases / 1_000_000
        ax[0,0].get_yaxis().set_major_formatter(
                tkr.FuncFormatter(lambda y, p: f"{y}M"))
    else:
        y = total_cases
        label = 'Cases'
        ax[0,0].get_yaxis().set_major_formatter(
                tkr.FuncFormatter(lambda y, p: f"{int(y):,d}"))
    ax[0,0].set_title('Total Cases')
    ax[0,0].grid(True)
    ax[0,0].plot(x, y, color='b')
    y = new_cases
    ax[0,1].set_title('New Cases')
    ax[0,1].get_yaxis().set_major_formatter(
            tkr.FuncFormatter(lambda y, p: f"{int(y):,d}"))
    ax[0,1].grid(True)
    ax[0,1].plot(x, y, color='b')
    y = total_deaths
    ax[1,0].set_title('Total Deaths')
    ax[1,0].get_yaxis().set_major_formatter(
            tkr.FuncFormatter(lambda y, p: f"{int(y):,d}"))
    ax[1,0].grid(True)
    ax[1,0].plot(x, y, color='b')
    y = new_deaths
    ax[1,1].set_title('New Deaths')
    ax[1,1].get_yaxis().set_major_formatter(
            tkr.FuncFormatter(lambda y, p: f"{int(y):,d}"))
    ax[1,1].grid(True)
    ax[1,1].plot(x, y, color='b')
    plt.savefig(fp, bbox_inches='tight') 
    plt.close(fig)

def update_readme():
    tc, td = total_cases[-1], total_deaths[-1]
    cases, deaths = new_cases[-1], new_deaths[-1]
    cmean, dmean = np.mean(new_cases[-7:]), np.mean(new_deaths[-7:])
    date = dates[-1]
    md = datetime.strptime(str(date), '%Y-%m-%d').strftime('%B %d')
    today = datetime.now().strftime('%B %d, %Y')
    today = f"{today}, {clck()} EST"
    df = pd.DataFrame({
        "U.S": ["Cases", "Deaths"], 
        "Total Reported": [f"{tc:,d}", f"{td:,d}"], 
        f"On {md}": [f"{cases:,d}", f"{deaths:,d}"], 
        "7-Day Average": [f"{int(cmean):,d}", f"{int(dmean):,d}"]
        })
    df = df.to_markdown(index=False, disable_numparse=True) 
    write_readme(README_TEMPLATE(), today, df)

def write_readme(template, date, df):
    print(f"writing to '{os.path.join(os.getcwd(), 'README.md')}'")
    with open('README.md', 'w') as f:
        f.write(template.format(date, df))

def clck():
    h = datetime.now().strftime('%H')
    h = int(h)
    m = datetime.now().strftime('%M')
    am = 'A.M'
    pm = 'P.M'
    if h < 12:
        if h == 0:
            h += 12
        c = f"{h}:{m} {am}"
    elif h >= 12:
        if h != 12:
            h -= 12
        c = f"{h}:{m} {pm}"
    return c

def mk_dir(*dirs):
    for d in dirs:
        if not os.path.isdir(d):
            print(f"creating '{os.path.join(os.getcwd(), d)}'")
            os.mkdir(d)

def parse(df, suffix):
    df = df.sort_values(by=[suffix])
    df = df[suffix]
    df = df.drop_duplicates()
    df = [df for df in df]
    return df

def push_git():
    if os.path.isdir('.git'):
        try:
            check_call('/usr/bin/git add .', shell=True)
            check_call('/usr/bin/git commit -m "Updating data."', shell=True)
        except Exception as error:
            print(f"\n{error}\n")
        acc = 0
        while True:
            try:
                check_call('/usr/bin/git push', shell=True)
                break
            except Exception as error:
                acc += 1
                retry(acc, error)

def fetch_all(urls):
    return [fetch(url) for url in urls]

def nan_to_mean(array):
    pos = []
    for i in range(len((a := array)) - 1):
        if a[i] >= 0:
            pos.append(a[i])
        else:
            if a[i+1] >= 0:
                x = pos[-1]
                y = a[i+1]
                a[i] = (y + x) / 2
                pos.append(a[i])
            elif a[i+2] >= 0:
                x = pos[-1]
                y = a[i+2]
                n = (y + x) / 2
                a[i] = (n + x) / 2
                a[i+1] = (y + x) / 2
                pos.append(a[i])
                pos.append(a[i+1])
            elif a[i+3] >= 0:
                x = pos[-1]
                y = a[i+3]
                n = (y + x) / 2
                a[i] = (n + x) / 2
                a[i+2] = (n + y) / 2
                a[i+1] = (a[i+2] + a[i]) / 2
                pos.append(a[i])
                pos.append(a[i+1])
                pos.append(a[i+2])

def plot_vacs(arrays, suffix, fn):
    x = dates
    y1 = total_doses 
    y2 = first_dose 
    y3 = second_dose
    fig, ax = plt.subplots(figsize=(12, 7), dpi=200)
    fig.suptitle(f"{suffix} COVID-19 Vaccinations")
    ax.grid(True)
    fig.autofmt_xdate()
    if second_dose[-1] >= 1_000_000:
        ax.get_yaxis().set_major_formatter(
                tkr.FuncFormatter(lambda y, p: f"{y / 1_000_000}M"))
    else:
        ax.get_yaxis().set_major_formatter(
          tkr.FuncFormatter(lambda y, p: f"{int(y):,d}")) 
    ax.plot(x, y1, label='Total Doses')
    ax.plot(x, y2, label='First Dose')
    ax.plot(x, y3, label='Second Dose')
    ax.legend()
    plt.savefig(fn, bbox_inches='tight')
    plt.close()

st = time()

while True:
    urls = (
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv', 
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv', 
            'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv', 
            'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv',
            'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations-by-manufacturer.csv'
            )
        
    ncd, scd, nvc, svc, man = fetch_all(urls)

    if any([ncd, scd, nvc, svc, man]) is False:
        timeout(3600)
    
    if ncd is not False:
        df = pd.read_csv(io.StringIO(ncd.decode('utf-8')))
        us_cols_dtypes = {
                'date': 'datetime64', 
                'cases': 'int64', 
                'deaths': 'int64'
                }
        dates, total_cases, total_deaths = get_arrays(df, us_cols_dtypes)
        new_cases, new_deaths = get_diffs(total_cases, total_deaths)
        usd = { 
                'date': dates,
                'total cases': total_cases,
                'total deaths': total_deaths,
                'new cases': new_cases,
                'new deaths': new_deaths
                }
        print(f"writing to '{os.path.join(os.getcwd(), 'us.csv')}'")
        write_csv(usd, 'us.csv')
        print(f"writing to '{os.path.join(os.getcwd(), 'us.png')}'")
        plot(usd.values(), 'U.S', 'us.png')
        update_readme()
    
    if scd is not False:
        d = pd.read_csv(io.StringIO(scd.decode('utf-8'))) 
        states = parse(d, 'state')
        mk_dir('states')
        print(f"writing to '{os.path.join(os.getcwd(), 'states')}'")
        for state in (t := tqdm(states, ncols=103, leave=False, ascii=' #')):
            t.set_description(state)
            df = d[d['state'].str.contains(f"^{state}$", case=False)]
            st_cols_dtypes = {
                'date': 'datetime64', 
                'state': 'U',
                'cases': 'int64',
                'deaths': 'int64'
                }
            dates, states, total_cases, total_deaths = get_arrays(df, st_cols_dtypes)
            new_cases, new_deaths = get_diffs(total_cases, total_deaths)
            std = {
                    'date': dates,
                    'state': states,
                    'total cases': total_cases,
                    'total deaths': total_deaths,
                    'new cases': new_cases,
                    'new deaths': new_deaths
                    }            
            write_csv(std, f"states/{state}.csv")
            plot(std.values(), state, f"states/{state}.png")
    
    if nvc is not False:
        df = pd.read_csv(io.StringIO(nvc.decode('utf-8')))
        df = df[df['location'].str.contains("United States", case=False)]
        us_cols_dtypes = {
                'date': 'datetime64',
                'people_vaccinated': 'float32',
                'people_fully_vaccinated': 'float32'
                }
        dates, first_dose, second_dose = get_arrays(df, us_cols_dtypes)
        nan_to_mean(first_dose)
        second_dose[0:25] = 0
        nan_to_mean(second_dose)
        first_dose = first_dose.astype(np.int64)
        second_dose = second_dose.astype(np.int64)  
        total_doses = np.array(first_dose + second_dose, dtype='int64')
        usv = {
                'date': dates,
                'total doses': total_doses,
                'first dose': first_dose,
                'second dose': second_dose
                }
        mk_dir('vaccinations')
        print(f"writing to '{os.path.join(os.getcwd(), 'vaccinations/us.csv')}'")
        write_csv(usv, 'vaccinations/us.csv')
        print(f"writing to '{os.path.join(os.getcwd(), 'vaccinations/us.png')}'")
        plot_vacs(usv.values(), 'U.S', 'vaccinations/us.png')

    if svc is not False:
        d = pd.read_csv(io.StringIO(svc.decode('utf-8'))) 
        states = parse(d, 'location')
        states.remove('Long Term Care')
        states.remove('United States')
        mk_dir('vaccinations', 'vaccinations/states')
        print(f"writing to '{os.path.join(os.getcwd(), 'vaccinations/states')}'")
        for state in (t := tqdm(states, ncols=103, leave=False, ascii=' #')):
            t.set_description(state)
            df = d[d['location'].str.contains(f"^{state}$", case=False)]
            st_cols_dtypes = {
                'date': 'datetime64', 
                'location': 'U',
                'people_vaccinated': 'float32',
                'people_fully_vaccinated': 'float32'
                }
            dates, states, first_dose, second_dose = get_arrays(df, st_cols_dtypes)
            if (np.isnan(first_dose[0])):
                first_dose[0] = 0
            if (np.isnan(second_dose[0])):
                second_dose[0] = 0
            nan_to_mean(first_dose)
            nan_to_mean(second_dose)
            first_dose = first_dose.astype(np.int64)
            second_dose = second_dose.astype(np.int64)
            total_doses = np.array(first_dose + second_dose, dtype='int64')
            stv = {
                    'date': dates,
                    'state': states,
                    'total doses': total_doses,
                    'first dose': first_dose, 
                    'second dose': second_dose,
                    }            
            write_csv(stv, f"vaccinations/states/{state}.csv")
            plot_vacs(stv.values(), state, f"vaccinations/states/{state}.png") 
    
    if man is not False:
        df = pd.read_csv(io.StringIO(man.decode('utf-8')))
        df = df[df['location'].str.contains("United States", case=False)]
        jj = df[df['vaccine'].str.contains('Johnson&Johnson', case=False)]
        pb = df[df['vaccine'].str.contains('Pfizer/BioNTech', case=False)]
        ma = df[df['vaccine'].str.contains('Moderna', case=False)]
        mk_dir('vaccinations')
        print(f"writing to '{os.path.join(os.getcwd(), 'vaccinations')}'")
        mf_cols_dtypes = {
                'date': 'datetime64', 
                'total_vaccinations': 'int64'
                }
        jj_dates, jj_total_vaccinations = get_arrays(jj, mf_cols_dtypes)
        pb_dates, pb_total_vaccinations = get_arrays(pb, mf_cols_dtypes)
        ma_dates, ma_total_vaccinations = get_arrays(ma, mf_cols_dtypes)
        x1 = pb_dates
        x2 = ma_dates
        x3 = jj_dates
        y1 = pb_total_vaccinations
        y2 = ma_total_vaccinations 
        y3 = jj_total_vaccinations 
        fig, ax = plt.subplots(figsize=(12, 7), dpi=200)
        fig.suptitle('Vaccines')
        ax.grid(True)
        fig.autofmt_xdate()
        if jj_total_vaccinations[-1] >= 1_000_000:
            ax.get_yaxis().set_major_formatter(
                    tkr.FuncFormatter(lambda y, p: f"{y / 1_000_000}M"))
        else:
            ax.get_yaxis().set_major_formatter(
              tkr.FuncFormatter(lambda y, p: f"{int(y):,d}")) 
        ax.plot(x1, y1, label='Pfizer / BioNTech')
        ax.plot(x2, y2, label='Moderna')
        ax.plot(x3, y3, label='Johnson & Johnson')
        ax.legend()
        plt.savefig('vaccinations/vaccines.png', bbox_inches='tight')

    if any([ncd, scd, nvc, svc, man]) is True:
        push_git() 
        timeout(3600)
