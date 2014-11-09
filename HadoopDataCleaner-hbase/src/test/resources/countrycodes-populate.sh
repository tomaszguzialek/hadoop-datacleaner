disable 'countrycodes'
drop 'countrycodes'
create 'countrycodes', {NAME => 'mainFamily'}

put 'countrycodes', 'DK', 'mainFamily:country_name', 'Denmark'
put 'countrycodes', 'DK', 'mainFamily:iso2', 'DK'
put 'countrycodes', 'DK', 'mainFamily:iso3', 'DNK'

put 'countrycodes', 'PL', 'mainFamily:country_name', 'PL'
put 'countrycodes', 'PL', 'mainFamily:iso2', 'PL'
put 'countrycodes', 'PL', 'mainFamily:iso3', 'POL'