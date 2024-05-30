import cdsapi
import calendar
from subprocess import call


def idmDownloader(task_url,folder_path,file_name):
    idm_engin=r"D:\Program Files (x86)\Internet Download Manager\IDMan.exe"#安装目录
    call([idm_engin,'/d',task_url, '/p', folder_path, '/f', file_name, '/a'])
    call([idm_engin, '/s'])


if __name__=='__main__':
    c=cdsapi.Client()
    dic = {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind', 'surface_pressure'],
        'year': '2022',
        'month': '',
        'day': [],
        'time': ['00:00', '01:00', '02:00',
                 '03:00', '04:00', '05:00',
                 '06:00', '07:00', '08:00',
                 '09:00', '10:00', '11:00',
                 '12:00', '13:00', '14:00',
                 '15:00', '16:00', '17:00',
                 '18:00', '19:00', '20:00',
                 '21:00', '22:00', '23:00'],
        'area':[34.5,117,28,126]
    }

    for m in range(8, 10):
        day_num = calendar.monthrange(2022, m)[1]
        dic['month'] = str(m).zfill(2)
        dic['day'] = [str(d).zfill(2) for d in range(1, day_num + 1)]
        #filename = r'...\'2022' + str(m).zfill(2) + '.nc'
        r = c.retrieve('reanalysis-era5-single-levels', dic, )
        url = r.location
        path=r'H:\era5'
        filename='2022'+str(m).zfill(2)+'.nc'
        idmDownloader(url,path,filename)
