#!/root/anaconda3/envs/venv/lib/PythonPkg/python.exe
import requests
import MySQLdb
import subprocess
import shapefile
import os, sys
#import YoConfigTool
import time
import datetime
import json
from osgeo import gdal

os.environ['PROJ_LIB'] = '/root/anaconda3/envs/venv/share/proj'
def CreateShapeFile():
    try:
        global shpFileName
        print("Creating Shape File Started...")
        db = MySQLdb.connect(host=dbhost,user=user,passwd=password,db=dbName,port=3306)
        cur1= db.cursor(MySQLdb.cursors.DictCursor)
        cur1.execute("SELECT "+geomFieldName+" FROM "+tableName)
        result_set = cur1.fetchall()
        shpFileName=processingFolderPath+uniqueString
        w =shapefile.Writer(shpFileName,shapefile.POLYGON)
        w.field('FID','C','40')
        w.autoBalance = 1
        cnt=0
        for row in result_set:
            cnt=cnt+1
            geomWkt=row["GEOMETRY"]
            geomWkt1=geomWkt.replace("PolygonZ","")
            b=geomWkt1.replace(")","")
            c=b.replace("(","")
            lst=c.strip().split(",")
            lst3=[]
            lst2=[]
            for a in lst:
                xys=a.strip().split(" ")
                x=float(xys[0])
                y=float(xys[1])
                lst1=[x,y]
                lst2.append(lst1)
            lst3.append(lst2)
            w.record(FID=cnt)
            w.poly(lst3)
        w.close()
        print("Number of shape created:"+str(cnt))
        return True
    except Exception as e:
        print(e.message)
        return False    
        
def DownloadImage(dateF):
    global tiffFileNamePath
    try:
        print("Dwnload image Started...")
        #dateF= datetime.datetime.utcnow().isoformat() + "Z"
        # dateF=datetime.datetime.today().strftime('%Y-%m-%d')+"T00:00:00.000Z"
        print(dateF)
        
        #image_url="http://copernicus.meteoromania.ro/geoserver/frizon/ows?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=frizon:soil_water_index&format=geotiff&subset=http://www.opengis.net/def/axis/OGC/0/Long(20.258928571427663,29.714285714284536)&subset=http://www.opengis.net/def/axis/OGC/0/Lat(43.6160714285712,48.26785714285678)&subset=http://www.opengis.net/def/axis/OGC/0/elevation(002)&subset=http://www.opengis.net/def/axis/OGC/0/time(\"2020-07-16T00:00:00.000Z\")"
        image_url="http://copernicus.meteoromania.ro/geoserver/frizon/ows?service=WCS&version=2.0.1&request=GetCoverage&CoverageId=frizon:soil_water_index&format=geotiff&subset=http://www.opengis.net/def/axis/OGC/0/Long(20.258928571427663,29.714285714284536)&subset=http://www.opengis.net/def/axis/OGC/0/Lat(43.6160714285712,48.26785714285678)&subset=http://www.opengis.net/def/axis/OGC/0/elevation(002)&subset=http://www.opengis.net/def/axis/OGC/0/time(\""+dateF+"\")"
        print(image_url)        
        r = requests.get(image_url,stream = True)
        tiffFileNamePath=processingFolderPath+uniqueString+".tif"
        with open(tiffFileNamePath,'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
        
                # writing one chunk at a time to pdf file
                if chunk:
                    f.write(chunk)
        fileSize=os.path.getsize(tiffFileNamePath)
        print(fileSize)
        if(fileSize<50):
            return False
        else:
            return True
        
    except Exception as e:
        print(e.message)
        return False    

# Below function is a workaround for performing ReSampling Technique using gdal for python
def performGDALProcess():
    reSampledFilePath = processingFolderPath+uniqueString+"_res.tif"
    maskedFilePath = processingFolderPath+uniqueString+"_output.tif"
    demProcessedFilePath = processingFolderPath+uniqueString+"_color.tif"
    colorFilePath = "/root/anaconda3/envs/venv/raster_process_delivery/color.txt"
    translatedFilePath = processingFolderPath+uniqueString+"_color.png"
    print('Re Sampling Started')
    gdal.Warp(reSampledFilePath, tiffFileNamePath,dstSRS='EPSG:4326',srcSRS='EPSG:4326',xRes='0.0004',yRes='0.0004',resampleAlg='near',format='GTiff')
    print('Re Sampling Completed')
    print('Mask By Shapefile Started')
    gdal.Warp(maskedFilePath, reSampledFilePath, format='GTiff', cutlineDSName=shpFileName+".shp", cutlineLayer=uniqueString, cropToCutline=True)
    print('Mask By Shapefile Completed')
    print('DEM Processing Started')
    gdal.DEMProcessing(demProcessedFilePath,maskedFilePath, "color-relief", colorFilename=colorFilePath)
    print('DEM Processing Completed')
    print('Translate Started')
    gdal.Translate(translatedFilePath,demProcessedFilePath, format='PNG', creationOptions=['worldfile=yes'])
    print('Translate Completed')
    
def MaskByPolygon():
    print("GDAL Process Started")
    performGDALProcess()
    print("GDAL Process Completed")
    gdalDirectoryPath = "/root/anaconda3/envs/venv/bin/"
    cmd3 = "rm /var/www/html/frizon/storage/app/public/swi/*.shp"
    cmd4 = "rm /var/www/html/frizon/storage/app/public/swi/*.tif"
    cmd5 = "rm /var/www/html/frizon/storage/app/public/swi/*.dbf"
    cmd6 = "rm /var/www/html/frizon/storage/app/public/swi/*.shx"
    f1 = open('/root/anaconda3/envs/venv/raster_process_delivery/file.sh','w')
    f1.write(cmd3)
    f1.write('\n')
    f1.write(cmd5)
    f1.write('\n')
    f1.write(cmd6)
    f1.write('\n')
    f1.write('echo other files deleted\n')
    f1.write('\n')
    f1.write('exit 0')
    f1.close
    pro= subprocess.Popen(['sudo', 'bash', '/root/anaconda3/envs/venv/raster_process_delivery/file.sh'],stdout=subprocess.PIPE) 
    return True
    
def getConfig(environment, cfgFilePath):
        """
            get configuration from file
        """
        try:
            with open(cfgFilePath) as outfile:
                jObj = json.loads(outfile.read())
            if(jObj != None):
                keys = jObj.get(environment,None)
                return keys
        except Exception as e:
            #raise Exception("get Config failed.." + str(e)).with_traceback(e.__traceback__)
            print(e.message)
            return False
                
def ReadConfig():
    global dir_path
    global cfgFilePath
    global environment
    global getconfig
    global dbhost
    global user
    global password
    global dbName
    global tableName
    global geomFieldName
    global processingFolderPath
    # global uniqueString
    global geoserverBaseUrl
    global gdalDirectoryPath
    global resampleSize   
    
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfgFilePath=dir_path+"/config.cfg"
    environment="PROD"
    print(cfgFilePath)
    #objconfig =YoConfigTool.ConfigTool(environment, cfgFilePath)
    getconfig = getConfig(environment, cfgFilePath)
    print(getconfig)
    # uniqueString = time.strftime("%Y%m%d_%H%M%S")
    # uniqueString = time.strftime("%Y%m%d")
    if getconfig != False:
        dbhost=getconfig.get("dbhost")
        user=getconfig.get("user")
        password=getconfig.get("password")
        dbName=getconfig.get("dbName")
        tableName=getconfig.get("tableName")
        geomFieldName=getconfig.get("geomFieldName")
        processingFolderPath=getconfig.get("processingFolderPath")
        geoserverBaseUrl=getconfig.get("geoserverBaseUrl")
        gdalDirectoryPath=getconfig.get("gdalDirectoryPath")
        resampleSize=getconfig.get("resampleSize")
        if ((resampleSize !="" and geoserverBaseUrl !="" and processingFolderPath!="" and dbhost !="" and user!="" and password!="" and dbName!="" and tableName!="" and geomFieldName!="")==False):
            return False
        print("Hello1")
        return True
    else:
        print("Error in Configuration reading")
        return False
    
base = datetime.datetime.today()
for numDays in range(5):
    date_x = base - datetime.timedelta(days=numDays)
    dateF = date_x.strftime('%Y-%m-%d')+"T00:00:00.000Z"   
    uniqueString = date_x.strftime('%Y%m%d')
    if __name__=='__main__':    
        chkReadConfig= ReadConfig()        
        if chkReadConfig:
            chkCreateShapeFile=CreateShapeFile()
            if chkCreateShapeFile:
                chkDownloadImage=DownloadImage()
                if chkDownloadImage:
                    chkMaskByPolygon= MaskByPolygon()
                    if chkMaskByPolygon:
                        print("Process Completed")
                        # sys.exit(0)
