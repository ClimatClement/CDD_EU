#Collection of homemade functions used by various scripts.

def Groupby_consecutive_ones(arr):
    '''
    INPUT
    arr DataArray 3-dimensionnal array of 1 and 0

    OUTPUT
    result DataArray 3-dimensionnal array. Cumulative sum of consecutive 1
    '''
    import numpy as np
    from itertools import groupby

    result = np.zeros_like(arr)
    idx = 0
    for key, group in groupby(arr):
        group_list = list(group)
        if key == 1:
            for i in range(len(group_list)):
                result[idx + i] = i + 1
        idx += len(group_list)
    return(result)

def Plot_EUR_map(arr,title:str='',cbar_label:str='',vmin:float=0,vmax:float=180,pal:str='Reds',pinpoints:dict={},savename:str=''):
    '''
    INPUTS
    arr         DataArray   2-dimensionnal
    title       str         The figure title
    cbar_label  str         The colorbar label
    vmin        float       The minimum value of the colorbar
    vmax        float       The maximum value of the colorbar
    pal         str         The pyplot color palette name
    savename    str         The path to save the figure (if not specified, the figure is displayed)

    OUTPUT
    Displays or saves the figure and prints the saved path
    '''
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    fig = plt.figure(figsize=(25, 12))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_title(title, fontsize=14, fontweight='bold')

    ax.coastlines()
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)
    ax.add_feature(cfeature.RIVERS, linewidth=0.5)
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')

    mesh = ax.pcolormesh(
        arr.longitude,
        arr.latitude,
        arr,
        transform=ccrs.PlateCarree(),
        cmap=pal,
        vmin=vmin,
        vmax=vmax
    )

    cbar = plt.colorbar(mesh, ax=ax, orientation='vertical', shrink=0.6, pad=0.05)
    cbar.set_label(cbar_label, fontsize=12)

    ax.set_extent([
        -25,
        41,
        35,
        72
    ], crs=ccrs.PlateCarree())

    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False
    gl.xlocator = mticker.FixedLocator(np.arange(-180, 181, 5))
    gl.ylocator = mticker.FixedLocator(np.arange(-90, 91, 5))

    if len([p for p in pinpoints])>0:
        p_lats = np.array([pinpoints[location]['lat'] for location in pinpoints])
        p_lons = np.array([pinpoints[location]['lon'] for location in pinpoints])
        p_names = [location for location in pinpoints]
        x, y, _ = ccrs.PlateCarree().transform_points(ccrs.Geodetic(), p_lons, p_lats).T
        ax.scatter(x, y, 100, color="k", marker="v", edgecolor="w", zorder=3)
        for i in range(len(p_names)):
            plt.text(x[i], y[i]+0.5, p_names[i], va="bottom", family="monospace", weight="bold")

    if savename!='':
        plt.savefig(savename)
        plt.close()
    else:
        plt.show()

def Plot_local_time_series(x,values,med_val,med_val_label,title,color:str='#907700'):
    '''
    CONSTRUCTION
    '''
    import numpy as np
    import matplotlib.pyplot as plt

    plt.bar(x,values,color=color)
    plt.plot(x,np.repeat(med_val,len(x)),linestyle=':',color='r',label=med_val_label)
    plt.legend()
    plt.title(title)
    plt.grid()
    plt.show()

def Write_to_log(log_file,log_text,overwrite=False):
    import os
    write_mode = 'w' if overwrite else 'a'
    if os.path.exists(log_file):
        with open(log_file, write_mode) as f:
                f.write(f"{log_text}\n")
    else:
        with open(log_file, 'w') as f:
                f.write(f"{log_text}\n")
