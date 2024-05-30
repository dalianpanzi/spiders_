import cdsapi
import os
import datetime
import re
from multiprocessing import Process, Pool


def check_file(path, filename):
    filePath=path
    files=os.listdir(filePath)
    ans=filename in files

    return ans

def datelist(start, end, step=None, remodel=None):
    start_date=datetime.datetime(*start)
    end_date=datetime.datetime(*end)
    result=[]
    curr_date=start_date
    if step is None or step.lower()=='day':
        fmt="%04d%02d%02d"
        fmt1="%Y%m%d"
    elif step.lower()=='month':
        fmt="%04d%02d"
        fmt1="%Y%m"
    elif step.lower()=='year':
        fmt="%04d"
        fmt1="%Y"

    if step is None or step.lower()=='day':
        while curr_date != end_date:
            result.append(fmt % (curr_date.year, curr_date.month, curr_date.day))
            curr_date+= datetime.timedelta(1)

        result.append(fmt %(curr_date.year, curr_date.month, curr_date.day))

    elif step.lower()=='month':
        result.append(fmt % (curr_date.year, curr_date.month))
        old_month=curr_date.month
        while curr_date != end_date:
            if curr_date.month != old_month:
                result.append(fmt %(curr_date.year, curr_date.month))
                old_month=curr_date.month
            curr_date += datetime.timedelta(1)

        if curr_date.month != old_month:
            result.append(fmt % (curr_date.year, curr_date.month))

    elif step.lower()=='year':
        result.append(fmt % (curr_date.year))
        old_year=curr_date.year
        while curr_date != end_date:
            if curr_date.year != old_year:
                result.append(fmt % (curr_date.year))
                old_year=curr_date.year
            curr_date += curr_date.year
        if curr_date.year != old_year:
            result.append(fmt %( curr_date.year))
    else:
        step_ele=re.search('(\S+)=\S+', step).group(1)
        step_num=re.search('\S+=(\S+)',step).group(1)
        dic={
            step_ele: int(step_num),
        }

        while curr_date !=(end_date+datetime.timedelta(**dic)):
            result.append(curr_date)
            curr_date += datetime.timedelta(**dic)

        return result
        exit()

    if remodel is None or remodel.lower()=='str':
        pass

    elif remodel.lower()=='date':
        dates=[]
        for time in result:
            current_date =datetime.datetime.strptime(time, fmt1)
            dates.append(current_date)
        result=dates
    return  result

def mkdir(path):
    path=path.strip()
    path=path.rstrip("\\")
    isExists = os.path.exists(path)

    if not isExists:
        os.makedirs(path)
        print(path +'is created successfully')
        return True
    elif isExists:
        print(path+'is exist')
        return False

def download_single_file(head, date, var):
    year, month, day= date.year, date.month, date.day
    save_path=head+'\\'+var
    mkdir(save_path)
    filename= str('%04d'%(year))+str('%02d'%(month))+str('%02d'%(day))+'_'+var+'.nc'

    if not check_file(save_path,filename):
        c=cdsapi.Client()

        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type':'reanalysis',
                'variable':[var,],
                'year': str('%04d'%(year)),
                'month': [str('%02d' % (month))],
                'day': [str('%02d' % (day))],
                'time': [

                    '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',

                    '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',

                    '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',

                    '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',

                ],
                'format': 'netcdf',
                'area': [34.5, 117, 28, 126]
                'grid': '0.25/0.25',
            },
            save_path+'\\'+filename
        )
    return 0

def main():
    date_list=datelist((2022,7,1),(2022,9,5),step='hours=24',remodel='date')
    variables=['10m_u_component_of_wind', '10m_v_component_of_wind', 'surface_pressure',]
    head=r'....' #路径
    date_var=[]
    for var in variables:
        for date in date_list:
            date_var.append([date,var])

    pool=Pool(4)#创建一个5个进程的进程池
    for item in date_var:
        pool.apply_async(func=download_single_file, args=(head, item[0], item[1]))
    pool.close()
    pool.join()


if __name__=='__main__':
    main()
