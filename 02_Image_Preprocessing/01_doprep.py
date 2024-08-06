from esa_snappy import ProductIO, HashMap, GPF
import os, gc, json, sys, time, datetime

############################################ 
def do_apply_orbit_file(source):
    print('\tApply orbit file...')
    parameters = HashMap()
    parameters.put('Apply-Orbit-File', True)
    output = GPF.createProduct('Apply-Orbit-File', parameters, source)
    return output

def do_thermal_noise_removal(source):
    print('\tThermal noise removal...')
    parameters = HashMap()
    parameters.put('removeThermalNoise', True)
    output = GPF.createProduct('ThermalNoiseRemoval', parameters, source)
    return output

def do_remove_grd_border_noise(source):
    print('\tRemove GRD border noise...')
    parameters = HashMap()
    parameters.put('Remove-GRD-Border-Noise', True)
    #parameters.put('borderLimit', 500)
    parameters.put('trimThreshold', .5)
    output = GPF.createProduct('Remove-GRD-Border-Noise', parameters, source)
    return output

def do_calibration(source, polarization, pols):
    print('\tCalibration...')
    parameters = HashMap()
    #parameters.put('outputSigmaBand', False)
    parameters.put('outputBetaBand', False)
    parameters.put('outputGammaBand', False)
    if polarization == 'DH':
        parameters.put('sourceBands', 'Intensity_HH,Intensity_HV')
    elif polarization == 'DV':
        parameters.put('sourceBands', 'Intensity_VH,Intensity_VV')
    elif polarization == 'SH' or polarization == 'HH':
        parameters.put('sourceBands', 'Intensity_HH')
    elif polarization == 'SV':
        parameters.put('sourceBands', 'Intensity_VV')
    else:
        print("different polarization!")
    parameters.put('selectedPolarisations', pols)
    parameters.put('outputImageScaleInDb', False)
    output = GPF.createProduct("Calibration", parameters, source)
    return output

def do_speckle_filtering(source):
    print('\tSpeckle filtering...')
    parameters = HashMap()
    parameters.put('filter', 'Refined Lee')
    #parameters.put('filterSizeX', 3)
    #parameters.put('filterSizeY', 3)
    output = GPF.createProduct('Speckle-Filter', parameters, source)
    return output

def do_terrain_correction(source, downsample):
    print('\tTerrain correction...')
    parameters = HashMap()
    parameters.put('demName', 'Copernicus 30m Global DEM')
    parameters.put('imgResamplingMethod', 'BICUBIC_INTERPOLATION')
    parameters.put('saveProjectedLocalIncidenceAngle', False)
    parameters.put('saveSelectedSourceBand', True)
    if downsample == 1:
        parameters.put('pixelSpacingInMeter', 20.0)
    output = GPF.createProduct('Terrain-Correction', parameters, source)
    return output

def lineartodb(source):
    print('\tLinear to DB Conversion...')
    parameters = HashMap()
    output = GPF.createProduct('LinearToFromdB', parameters, source)
    return output

def do_operate(input,output):
    print("=============================================")
    gc.enable()
    gc.collect()
    sentinel_1 = ProductIO.readProduct(input)
    print(sentinel_1)
    loopstarttime=str(datetime.datetime.now())
    print('Start time:', loopstarttime)
    start_time = time.time()
    filename=input.split('/')[-1].split('_')
    polarization=filename[3][2:]
    if polarization == 'DV':
        pols = 'VH,VV'
    elif polarization == 'DH':
        pols = 'HH,HV'
    elif polarization == 'SH' or polarization == 'HH':
        pols = 'HH'
    elif polarization == 'SV':
        pols = 'VV'
    else:
        print("Polarization error!")
    applyorbit = do_apply_orbit_file(sentinel_1)
    thermaremoved = do_thermal_noise_removal(applyorbit)
    grdborder = do_remove_grd_border_noise(thermaremoved)
    calibrated = do_calibration(grdborder, polarization, pols)
    down_speckled=do_speckle_filtering(calibrated)
    down_corrected=do_terrain_correction(down_speckled,1)
    convert=lineartodb(down_corrected)
    print("Writing...")
    ProductIO.writeProduct(convert, output,'GeoTIFF')
    del applyorbit
    del thermaremoved
    del grdborder
    del calibrated
    del down_speckled
    del down_corrected
    sentinel_1.dispose()
    sentinel_1.closeIO()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("=============================================")

def do_check(filename):
    input='/data/ksa/01_Image_Acquisition/01_Raw_Image/'+filename.split('-')[0]+'.zip'
    output=input.replace('01_Raw_Image','02_Processed_Image').replace('.zip','')
    if os.path.isfile(output+'.tif'):
        print('File has been processed. Skip')
    else:
        do_operate(input,output)
        os.remove(input)
    
def main():
    kdprov=sys.argv[1]
    metadata='/data/ksa/01_Image_Acquisition/04_Json_Raw_Download/'+kdprov+'_metadata_ASF.json'
    with open(metadata,'r') as f:
        dt_prov=json.load(f)
    for i in range(0,len(dt_prov['features'])):
        file_name=dt_prov['features'][i]['properties']['fileID']
        print('*********************************************************')
        print('File: ',i,'/',len(dt_prov['features'])-1)
        print('Start for ',file_name)
        do_check(file_name)
        print('Finish ')
        print('*********************************************************')

if __name__ == "__main__":
    main()

        