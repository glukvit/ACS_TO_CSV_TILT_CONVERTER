#!/usr/bin/env python3
import re #регулярка
import glob
import datetime
import time
#import pdb
import csv
import os.path
import os
#import plotly.express as px
#import plotly as py
#from plotly.subplots import make_subplots
#import plotly.graph_objects as go
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import math
#import Plot_viz_beta7
#from leb import lebush as lb

#!!!!!!!
#Скрипт берет набор ASC файлов и делает из них три датафрема как в в обычной обработке полный датафрейм, часовой и недельный

Desteny='/home/gluk/TEMP_DATA/IVS1/ASC' #Здесь лежат исходные ASC
conf_fl='/home/gluk/bin/PROJECTS/ASC_TO_CSV_TILT_CONVERTER/conf_koef.txt' #Файл с коэффициентами станций
lvl_pth='/home/gluk/config/IVST_level_correction.txt'
#LEBpath='/home/gluk/Compen'# Здесь лежит прога Любушина
#HTMLpath='/home/gluk/TILTS_ONLINE/'#Сюда запишем готовые html графики

STname='IVST' #Название станции

#Параметры, которые будут передаваться модулю Plot_viz_beta7 для построения графиков

#Hourfl1='IVST_hour1.csv' #Название часового файла, который будем обрабатывать прогой Любушина

#Hourfl='IVST_hour.csv' #Название часового файла, который будем обрабатывать прогой Любушина
#Mainfl='IVST.csv' #Название основного файла для построения графиков
#Weekfl='IVST_week.csv'#Название недельного файла для построения графиков

#Hour_viz_path=Desteny+Hourfl#Передаем модулю Plot_viz_beta7 для построения часового файла
#Main_viz_path=Desteny+Mainfl#Передаем модулю Plot_viz_beta7 для построения часового файла
#Week_viz_path=Desteny+Weekfl#Передаем модулю Plot_viz_beta7 для построения часового файла

TIME=pd.Series()
SECTime=pd.Series()
HAEs=pd.Series()
HANs=pd.Series()
HK2s=pd.Series()

hae=[]
han=[]
hk2=[]

def lnf(): #Получения списка ACS-файлов из директории Desteny, где находятся загруженные файлы. Вызывается первой
    crnt=os.getcwd() # текущая директория
    os.chdir(Desteny) #Меняем директорию на Desteny
    ln=glob.glob('*.ASC') #Получаем список файлов в Desteny
    ln=' '.join(ln) #Объеденили список ln в str
    os.chdir(crnt)#Вернулись в текущую директорию
    return(ln) #Возвращаем str со списком файлов

def sort1(ls):	#Сортируем файлы из списка полученного с помощью процедуры lnf(). Вызывается второй после lnf

    ls=ls.split(' ')
    for every in ls:
        if 'HAE' in every:
            hae.append(every)
        elif 'HAN' in every:
            han.append(every)
        elif 'HK2' in every:
            hk2.append(every)
  
    return(hae,han,hk2)
def koef(dt,nm,ch): #Прикручиваем коэффициенты к отсчетам! На входе 1)данные в отсчетах, 2) Имя станции, 3) Имя канала. Процедура вызывается из rddta

    with open(conf_fl,'r') as file: #Открываем файл с коэффициентами 
		    for line in file: #Читаем строку
			    string=line.split() #Превращаем строку в list
			    print(string)
			    if string[0]==nm: #Если данная строка содержит коэффициенты нужной станции то:
				    if ch=='HAE': #Сравниваем имя обрабатываемого канала с необходимым 
					    koef=float(string[1])  #Это если канал HAE берем первый коэффициент из строки 
#					    print('HAE',koef)
				    elif ch=='HAN': # Это если канал HAN  берем второй коэффициент из строки
					    koef=float(string[2])
#					    print('HAN',koef)
				    else:   # Это если канал HK2 берем третий коэффициент из строки
					    koef=float(string[3])
#					    print('HK2',koef)
    data=[float(item)*koef for item in dt] # Берем данные канала (dt), превращаем каждое значение во float и умножаем на коэффициент. В результате получаем список data в микрорадианах
    file.close()
    return(data) #Возвращаем процедуре rddta список с данными в микрорадианах

def rddta(evr,fnm,cl):#Чтение непосредственно данных из файла
    f=open(evr, 'r')#снова открываем файл
    i=1 #счетчик пропуска строк нужно пропустить 2 строки
    data=[]
    while i<3: #пропускаем строки
	    line=f.readline()      #Просто прочитываем строки ничего не делая.
	    i=i+1#Увеличиваем счетчик
    while line:		# Читаем файл пока есть строки, очищаем добавляем данные в переменную data
	    try:
		    line=f.readline()#читам построчно файл
		    line=line.replace('\n','') #удаляем мусор \n из переменной
		    data.append(line) #добавляем строку в список data
	    except IndexError:
		    pass #Ну происходит ошибка а мы ничего делать не будем. Ибо данные на месте.
    f.close() #Закрываем текущий файл.
    data.remove('') #Удаляем лишние пробелы
    data=koef(data,fnm,cl)#Прикручиваем аппаратурные коэффициенты
    return(data)

    return()
def readdata(evr,chl,TIME,SECTime,HAEs,HANs,HK2s): #Читаем данные из списка файлов определенногоканала. На вход список файлов канала и имя канала. Вызывается третьей после sort1
    print('Чтение данных, процедура readdata')
    evr.sort() # Сортируем список. В данном случае по дате.
    evr=str(evr) # Превращаем список в стринг
    evr=evr.split(',') #Разбиваем стринг с именами файлов канала на части, чтобы подставить в цикл
    os.chdir(Desteny) #Меняем директорию
    for every in evr: # Цикл обработки и чтения данных для каждого файла канала
	    ''' Удаляем лишнее из каждой части стринга содержащего
		имя открываемого файла
	    '''
	    every=every.replace('[','')
	    every=every.replace('\'','')
	    every=every.replace(' ','')
	    every=every.replace(']','')

	    flnm='IVS1'
	    name=flnm+"_"+chl+".csv" #Создаем имя файла csv в который запишем данные формат: имя + канал + расширение
	    name1=flnm+"_"+chl+"_"+"1week"+".csv" #Создаем имя csv файла в который запишем данные за неделю
	    f=open(every, 'r') #Открываем каждый файл
	    ln=(len(f.readlines()))-2 # Определяем количество строк файла и вычитаем первые две строки для определения длины данных
	    f.close()	#Закрываем файл
	    f=open(every,'r') #Снова открываем файл для чтения
	    lines=f.readlines() #Читаем все строки
	    x=lines[1] #Переходим к строке с шапкой файла
	    ntime=x[0:14] #Присваиваем переменной значение начала отсчета
	    intrvl=int(x[33:36]) #Частота дискретизации в секундах 
	    f.close()
	    tm=datetime.datetime.strptime(ntime, "%Y%m%d%H%M%S") #преобразует начало отчета в тип datetime соответствующего вида.
	    b=tm#дежурная переменная с началом отсчета тип datetime
	    rslt=rddta(every,flnm,chl)# получаем от процедуры список с данными (отсчеты) Его будем подставлять в конечный файл во второй стол
	    timing=[]#вводим список в котором передадим процедуре список таймингов текущего файла. Его будем подставлять в конечный файл в
	    dectiming=[]#вводим список для десятичной даты. Его будем подставлять в конечный файл
	    for ever in range(ln): #перебираем длину файла чтобы получить список таймингов
#		    b1=datetime.datetime.strftime(b, "%Y:%m:%d:%H:%M:%S") #отсчеть в datetime переводим в стринг !!!!БЫЛО ТАК!!!
		    b1=datetime.datetime.strftime(b, "%Y:%m:%d:%H:%M") #отсчеть в datetime переводим в стринг
		    tosec=datetime.datetime.strptime(b1,"%Y:%m:%d:%H:%M")
		    dctm=int(tosec.timestamp()) #Перевод в каждого отсчета в количество секунд с начала эпохи!!!! 43200 перевод из мустного времени в UTC
		    dectiming.append(dctm)# добавляем каждый отсчет в секундах в список таймингов в секундах 
		    timing.append(b1) #добавляем отсчеты в список тайминга в стринге. Это список таймингов
		    b=tm+datetime.timedelta(seconds=intrvl) #берем время следующего отсчета и прибавляем к нему интервал по
		    tm=b #теперь текущий первый отсчет а стал текущим отсчетом

	    if chl=='HAE': #Создаем pd.Series для каждого канала 
		    a=pd.Series(rslt) #Временная переменная в которую помещаются данные при прочтении очередного файла в цикле
		    HAEs=pd.concat([HAEs,a],ignore_index=True) # Series для каждого канала берем series и добавлям в конец новый кусок из файла 
		    a=pd.Series(timing) #Временная переменная для создания Series тайминга
		    TIME=pd.concat([TIME,a],ignore_index=True) #Series для тайминга отсчетов
		    a=pd.Series(dectiming)#Временная переменная для создания Series тайминга в секундах
		    SECTime=pd.concat([SECTime,a],ignore_index=True) #Series для тайминга отсчетов в секундах
		    tmm=TIME #Только при  первой иттерации цикла при обработке файлов HAE забираем тайминги
		    dct=SECTime#Только из первого цикла при обработке файлов HAE забираем секундные тайминги
		    data=HAEs#Только из первого цикла при обработке файлов HAE забираем данные HAE
	    elif chl=='HAN':
		    a=pd.Series(rslt) #Временная переменная в которую помещаются данные при прочтении очередного файла в цикле
		    HANs=pd.concat([HANs,a],ignore_index=True) # Series для каждого канала берем series и добавлям в конец новый кусок из файла.
		    tmm=0#При втоой иттерации цикла не берем тайминги
		    dct=0#При втоой иттерации цикла не берем тайминги в секундах 
		    data=HANs# При второй иттерации цикла при обработке файлов HAN забираем данные HAN
	    else:
		    a=pd.Series(rslt)#Временная переменная в которую помещаются данные при прочтении очередного файла в цикле
		    HK2s=pd.concat([HK2s,a],ignore_index=True)# Series для каждого канала берем series и добавлям в конец новый кусок из файла
		    tmm=0#При третьей иттерации цикла не берем ничего 
		    dct=0
		    data=HK2s #При третьей иттерации цикла берем только HK2
    print('Последний обрабатываемый файл',every)
    print('Завершение readdata')
    return(tmm,dct,data) #tmm дата, dct отсчеты в секундах с начала эпохи, data данные по каналу

def errordet(indf): #Фильтр грубых ошибок удаляем тройную сигма На вход матрицу из def t_comNdetr. На вход датафрейм из levcor
    
    print('Фильтр грубых ошибок, процедура errordet')
    stdHAE=(indf['HAE'].mean())# Получаем стандартное отклонение по HAE
    stdHAN=(indf['HAN'].mean())# Получаем стандартное отклонение по HAN

    sigmaHAE_3=stdHAE*3 #Три сигма HAE
    sigmaHAN_3=stdHAN*3 #Три сигма HAN

    indf.drop(labels=np.where(abs(indf['HAE'])>abs(sigmaHAE_3))[0], inplace=True)#Удаляем строки из датафрейма по условию для HAE

    indf.reset_index(inplace=True)#Устанавливаем новые индексы
    indf.drop('index',axis=1,inplace=True)#Удаляем созданный столбец со старыми индексами

    indf.drop(labels=np.where(abs(indf['HAN'])>abs(sigmaHAN_3))[0],inplace=True)#Повторно берем датафрейм и удаляем строки по условию для HAN
    indf.reset_index(inplace=True)#Устанавливаем новые индексы

    outdf=indf
    outdf.drop('index',axis=1,inplace=True)#Удаляем созданный столбец со старыми индексами

    print('Завершения  errordet, запускаем t_comp')
    
    t_comp(outdf) #Датафрейм на вход t_comp
    return()#Возращаем датафрейм после фильтра грубых ошибок.

def levcor(TIMf,DTMf,HAEf,HANf,HK2f): #Корректировка уровней после исправления на станции. На вход дата, секунды, три канала
    print('Вошли в levcor')
    sz=os.path.getsize(lvl_pth) #Проверяем размер файла коррекции
    print('Размер файла коррекции', sz)
    if sz !=0: #Если файл коррекции не нулевой:
	    indf=pd.DataFrame({'DATE':TIMf,'DECDATE':DTMf,'HAE':HAEf,'HAN':HANf,'HK2':HK2f}) #Создаем входной датафрейм

	    crdf11=pd.DataFrame() #Временные датафреймы
	    crdf22=pd.DataFrame()
	    tempdf=pd.DataFrame()
	    tempdf=pd.DataFrame({'oldHAE':indf['HAE'],'oldHAN':indf['HAN'],'HK2':indf['HK2']}) #Временный датафрейм со входными данными
#    numcors=len(re.findall(r"[\n']+", open(lvl_pth).read())) #Считает количество строк пропуская пустые строки.
    
	    with open(lvl_pth,'r') as file: #Открываем файл коррекции
		    for line in file.readlines(): #Читаем строку
			    string=line 
			    string=string.split() #Превращаем в строку
			    HAEcf=float(string[1]) #Выбираем уровень коррекции канала  HAE по позиции в строке
			    HANcf=float(string[2]) #Выбираем уровень коррекции канала  HAN по позиции в строке
			    dcdt=int(string[0])  
			    crdf1=indf[indf['DECDATE'] <= dcdt]
			    crdf2=indf[indf['DECDATE'] > dcdt]
			    crdf11=pd.DataFrame({'DATE':crdf1['DATE'],'DECDATE':crdf1['DECDATE'],'HAE':crdf1['HAE'],
						'HAN':crdf1['HAN'],'HK2':crdf1['HK2']
						})
			    crdf22=pd.DataFrame({'DATE':crdf2['DATE'],'DECDATE':crdf2['DECDATE'],'HAE':crdf2['HAE']+HAEcf,
						'HAN':crdf2['HAN']+HANcf,'HK2':crdf2['HK2']
						 })
			    indf=pd.concat([crdf11,crdf22])
		    
	    outdf=pd.DataFrame({'DATE':indf['DATE'],'DECDATE':indf['DECDATE'],'HAE':indf['HAE'],'HAN':indf['HAN'],
				'HK2':indf['HK2'],
				'raw_HAE':tempdf['oldHAE'],'raw_HAN':tempdf['oldHAN']
				 })
    else:
	    outdf=pd.DataFrame({'DATE':TIMf,'DECDATE':DTMf,'HAE':HAEf,'HAN':HANf,'HK2':HK2f})
    errordet(outdf) #На выходе вне зависимости от наличия файла коррекции датафрейм подается в на вход errordet
    return()


def t_comp(indf):#Термокомпенсация и детренд. На вход время, время в сек.,три канала. На вход датафрейм из errodet
    print('Термокомпенсация рядов, процедура в t_comp')

    indf['HAE_TCOMP']=indf['HAE']+indf['HAE']*0.0004*(indf['HK2']-25.8)-1.5*(indf['HK2']-25.8)
    indf['HAN_TCOMP']=indf['HAN']+indf['HAN']*0.0004*(indf['HK2']-25.8)-1.5*(indf['HK2']-25.8)

    outdf=indf

    print('Переход к детрендированию')
    detrend(outdf) #Датафрейм на вход детренда 
    return()

#Детренд
def detrend(indf): #Датафрейм на вход из t_comp
    print('Детрендирование рядов, процедура detrend')

    X = [i for i in range(0, indf.shape[0])]#создаем список с номерами отсчетов длиной равный длине столбца

    X = np.reshape(X, (len(X), 1)) #Превращаем список с номерами отсчетов в столбец
        
    HAEd=indf['HAE_TCOMP'] #DataSeries со термокомпенсацией канала HAE_TCOMP
    HANd=indf['HAN_TCOMP'] #DataSeries со термокомпенсацией канала HAN_TCOMP
    
    modelHAE = LinearRegression() #Объявление переменной линейной регрессии для канала HAE_TCOMP с помощью библиотеки scikit
    modelHAN = LinearRegression() #Объявление переменной линейной регрессии для канала HAE_TCOMP с помощью библиотеки sсikit
    
    modelHAE.fit(X,HAEd) #Коэффициенты полинома X столбец с номерами отсчетов, HAEd канал с термокомпенсацией
    modelHAN.fit(X,HANd) #Коэффициенты полинома X столбец с номерами отсчетов, HAТd канал с термокомпенсацией
    
    trendHAE = modelHAE.predict(X) #Тренд канала. 
    trendHAN = modelHAN.predict(X) #Тренд канала.

    detrendedHAE = [HAEd[i]-trendHAE[i] for i in range(0,indf.shape[0])] #Удаление тренда
    detrendedHAN = [HANd[i]-trendHAN[i] for i in range(0,indf.shape[0])] #Удаление тренда
    
    indf['HAE_T_COM-DETREND']=detrendedHAE #Добавляем в основной df столбцы с детрендом
    indf['HAN_T_COM-DETREND']=detrendedHAN
    print('Запись окончательного файла построения графиков')
    indf.to_csv('/home/gluk/TEMP_DATA/IVS1/IVST.csv', index=True) #Записываем весь датафрейм в файл
    df2=indf.tail(10080)#-10080
    print('Запись файла с данными за последнюю неделю')
    df2.to_csv('/home/gluk/TEMP_DATA/IVS1/IVS1_week.csv', index=True)
    outdf=indf
    print('Децимация данных')
    decim(outdf) #Далее последний этап путешествия датафрейма децимация
    
    return()
#Децимация
def decim(df): # Датафрейм из detrend. Это последний этап
    print('Децимация матрицы, процедура decim')

    # !!!!!!! ЗДЕСЬ ВНИМАТЕЛЬНО!!! ОБРАТИТЬ ВНИМАНИЕ НА ЧАСОВЫЕ ПОЯСА ЕСЛИ ИСПОЛЬЗОВАТЬ НА ДРУГОМ КОМПЕ!!!
    
    df['PD_DATE']=pd.to_datetime(df['DECDATE']+43200, unit='s') #Время в секундах переводим в формат datetime + разница в часовых поясах в секундах и записываем в столбец
    df.set_index(['PD_DATE']).resample('1H').mean()# Выборка по столбцу времени.Устанавливаем индекс на столбец.  Выборка 1 час. Данные во всех столбцах осредняются
    df.reset_index(inplace=True) # Сбрасываем столбец индексов. Теперь индексы по умолчанию
    df['HAE'].replace('',np.nan, inplace=True)#Из за пропусков в данных появляются дыры. Заполняем дыры значениями nan
    df.dropna(subset=['HAE'], inplace=True) #Проверяем значения nan по столбцу HAE, по идее если он имеет nan, то останые тоже
    df.reset_index(inplace=True)
#    df.rename(columns={'PD_DATE':'DATE'}, inplace=True) #Переименоваем колонку времени PD_DATE становится DATE
    del df['PD_DATE']
    del df['index']
    print(df.columns)
    print('Завершение децимации и запись часового файла')

    df.to_csv('/home/gluk/TEMP_DATA/IVS1/IVS1_hour.csv', index=True) #Записываем часовой файл и все. Конец работы скрипта.

    return()

a=lnf() #Получаем список исходных файлов из директории Desteny
hae,han,hk2=sort1(a) #Передаем переменным сразу три списка из процедуры sort, которой в качестве параметра передаем переменную с директорией где лежат файлы
TM,DT,HE=readdata(hae,'HAE',TIME,SECTime,HAEs,HANs,HK2s) #Передаем список с файлами канала HAE и имя канала
TM1,DT1,HN,=readdata(han,'HAN',TIME,SECTime,HAEs,HANs,HK2s) #Передаем список с файлами канала HAN и имя канала
TM2,DT2,HK=readdata(hk2,'HK2',TIME,SECTime,HAEs,HANs,HK2s) #Передаем список с файлами канала HK2 и имя канала
levcor(TM,DT,HE,HN,HK)

##          Все что ниже для конвертации ASC >> CSV не нужно.           !!!!!!!

#lb(Desteny,LEBpath,STname,Hourfl) #Отдельный модуль конкретно здесь не используется этот скрипт только для перевода ASC в CSV
#print('Последовательный вызов модуля Plot_viz_beta7 для построения графиков')
#Plot_viz_beta7.mainplot(Desteny,HTMLpath,Main_viz_path,STname)
#Plot_viz_beta7.hourplot(Desteny,HTMLpath,Hour_viz_path,STname)
#Plot_viz_beta7.weekplot(Desteny,HTMLpath,Week_viz_path,STname)
