temp_indicators = [
    'annual_tasmin',
    'annual_tasmax',
    'tavg-tasmin_tasmax',
    'hdd65f-tasmin_tasmax',
    'cdd65f-tasmin_tasmax',
    'frostfree_tasmin',
    'gt-q99_tasmax'
]

pr_indicators = [
    'dryspells_pr',
    'annual_pr',
    'gt-q99_pr',
]

ensembles = [
    'q50',
    'q25',
    'q75',
    'iqr'
]

scenarios = [
    'rcp45',
    'rcp85'
]

startyear = 2000
endyear = 2080

def main():

    for i in temp_indicators:
        for s in scenarios:
            for y in range(startyear, endyear+1, 10):
                for ch in ['abs', 'diff']:
                    for e in ensembles:
                        y1 = y-15
                        y2 = y+15
                        print(f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}')

    for i in pr_indicators:
        for s in scenarios:
            for y in range(startyear, endyear+1, 10):
                for ch in ['abs', 'ch']:
                    for e in ensembles:
                        y1 = y-15
                        y2 = y+15
                        print(f'{e}-{ch}-{i}_{s}_ens_{y1}-{y2}')

if __name__ == '__main__':
    main()
