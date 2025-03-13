import sqlite3
import json
import os

# Paths
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'zugdienste.db')
WEB_DIR = os.path.dirname(__file__)
OUTPUT_HTML = os.path.join(WEB_DIR, 'train_filter.html')

# HTML template (unchanged, included for completeness)
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Train Services Filter</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            max-width: 1200px;
            margin: 0 auto;
            background-color: #1e1e1e;
            color: #ffffff;
        }
        .filter-container {
            display: flex;
            background-color: #2d2d2d;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 5px;
        }
        .left-panel, .center-panel, .right-panel {
            margin-right: 15px;
        }
        .left-panel { width: 30%; }
        .center-panel { width: 40%; display: flex; flex-direction: column; }
        .right-panel { width: 30%; }
        .checkbox-list { border: 1px solid #555; padding: 5px; background-color: #333; }
        .scrollable-checkbox-list { max-height: 500px; overflow-y: auto; border: 1px solid #555; padding: 5px; background-color: #333; }
        .checkbox-item { margin: 5px 0; }
        .checkbox-item.disabled label { color: #777; }
        .filter-row { margin-bottom: 10px; display: flex; align-items: center; }
        .start-end-row { display: flex; justify-content: space-between; margin-bottom: 10px; }
        .start-end-group { width: 48%; }
        .start-end-header { display: flex; align-items: center; margin-bottom: 5px; }
        label { margin-right: 10px; width: 100px; }
        table { width: 100%; max-width: 1200px; border-collapse: collapse; margin: 20px auto; background-color: #1e1e1e; }
        th, td { padding: 8px; border: 1px solid #444; text-align: left; font-size: 12px; }
        th { background-color: #3c3c3c; }
        tr:nth-child(even) { background-color: #2a2a2a; }
        select, input[type="text"] { padding: 5px; margin-right: 10px; border-radius: 3px; border: 1px solid #555; background-color: #333; color: #fff; }
        input[type="checkbox"] { margin-right: 5px; }
        input[type="checkbox"]:disabled { opacity: 0.5; }
        .results-count { margin: 10px 0; font-weight: bold; text-align: center; }
        #debug { color: #ff4444; font-size: 12px; }
        button { padding: 5px; margin-right: 10px; background-color: #444; color: #fff; border: 1px solid #555; border-radius: 3px; cursor: pointer; }
        button:hover { background-color: #555; }
        .time-group span, .stops-group span { margin-right: 5px; }
        .select-group { margin-top: 5px; }
        .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5); }
        .modal-content { background-color: #2d2d2d; margin: 15% auto; padding: 20px; border: 1px solid #555; width: 50%; max-height: 60%; overflow-y: auto; }
        .close { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
        .close:hover { color: #fff; }
    </style>
</head>
<body>
    <h1>Train Services Filter</h1>
    <div id="debug"></div>
    
    <div class="filter-container">
        <div class="left-panel">
            <label title="country">Countries:</label>
            <div class="checkbox-list" id="country-list"></div>
            <div class="select-group">
                <span>Select:</span>
                <button id="country-select-all">All</button>
                <button id="country-deselect-all">None</button>
            </div>
            <br>
            <label title="route">Routes:</label>
            <div class="checkbox-list" id="route-list"></div>
            <div class="select-group">
                <span>Select:</span>
                <button id="route-select-all">All</button>
                <button id="route-deselect-all">None</button>
            </div>
        </div>
        <div class="center-panel">
            <div class="filter-row">
                <label title="art">Type:</label>
                <select id="filter-art">
                    <option value="">All</option>
                    <option value="P">Passenger (P)</option>
                    <option value="C">Cargo (C)</option>
                </select>
            </div>
            <div class="filter-row">
                <label title="gattung">Category:</label>
                <input type="text" id="filter-gattung" placeholder="e.g., IC or ME RE">
                <label><input type="checkbox" id="exact-gattung"> Exact</label>
            </div>
            <div class="filter-row">
                <label title="zugnr">Train Number:</label>
                <input type="text" id="filter-zugnr" placeholder="Enter train number">
                <label><input type="checkbox" id="exact-zugnr"> Exact</label>
            </div>
            <div class="filter-row">
                <label title="br">BR:</label>
                <input type="text" id="filter-br" placeholder="e.g., 101">
                <label><input type="checkbox" id="exact-br"> Exact</label>
            </div>
            <div class="filter-row">
                <label title="begin">Start Time:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-begin-min" placeholder="HH:MM" size="3">
                    <span>Max</span><input type="text" id="filter-begin-max" placeholder="HH:MM" size="3">
                </div>
            </div>
            <div class="filter-row">
                <label title="fahrzeit">Duration:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-fahrzeit-min" placeholder="HH:MM" size="3">
                    <span>Max</span><input type="text" id="filter-fahrzeit-max" placeholder="HH:MM" size="3">
                </div>
            </div>
            <div class="filter-row">
                <label title="nhalte">Stops:</label>
                <div class="stops-group">
                    <span>Min</span><select id="filter-nhalte-min"></select>
                    <span>Max</span><select id="filter-nhalte-max"></select>
                </div>
            </div>
            <div class="filter-row">
                <label title="s_km">Distance:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-skm-min" size="2">
                    <span>Max</span><input type="text" id="filter-skm-max" size="2">
                </div>
            </div>
            <div class="filter-row">
                <label title="dv">Avg Speed:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-dv-min" size="2">
                    <span>Max</span><input type="text" id="filter-dv-max" size="2">
                </div>
            </div>
            <div class="filter-row">
                <label title="laenge">Length:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-laenge-min" size="2">
                    <span>Max</span><input type="text" id="filter-laenge-max" size="2">
                </div>
            </div>
            <div class="filter-row">
                <label title="masse">Mass:</label>
                <div class="time-group">
                    <span>Min</span><input type="text" id="filter-masse-min" size="2">
                    <span>Max</span><input type="text" id="filter-masse-max" size="2">
                </div>
            </div>
            <div class="start-end-row">
                <div class="start-end-group">
                    <div class="start-end-header">
                        <label title="start">Start Types:</label>
                        <label><input type="checkbox" id="filter-start-halt" title="start_halt"> Timed</label>
                    </div>
                    <div class="scrollable-checkbox-list" id="start-list"></div>
                    <div class="select-group">
                        <span>Select:</span>
                        <button id="start-select-all">All</button>
                        <button id="start-deselect-all">None</button>
                    </div>
                </div>
                <div class="start-end-group">
                    <div class="start-end-header">
                        <label title="ende">End Types:</label>
                        <label><input type="checkbox" id="filter-end-halt" title="end_halt"> Timed</label>
                    </div>
                    <div class="scrollable-checkbox-list" id="end-list"></div>
                    <div class="select-group">
                        <span>Select:</span>
                        <button id="end-select-all">All</button>
                        <button id="end-deselect-all">None</button>
                    </div>
                </div>
            </div>
            <div class="filter-row">
                <label title="ev">Has Events:</label>
                <input type="checkbox" id="filter-ev">
            </div>
            <div class="filter-row">
                <label title="w1">Turnarounds:</label>
                <div class="stops-group">
                    <span>Min</span><select id="filter-w1-min"></select>
                    <span>Max</span><select id="filter-w1-max"></select>
                </div>
            </div>
        </div>
        <div class="right-panel">
            <label title="br">BR List:</label>
            <div class="scrollable-checkbox-list" id="br-list"></div>
            <div class="select-group">
                <span>Select:</span>
                <button id="br-select-all">All</button>
                <button id="br-deselect-all">None</button>
            </div>
        </div>
    </div>

    <div class="results-count" id="results-count">Showing 0 results</div>
    <table id="results-table">
        <thead>
            <tr>
                <th data-col="zugnr">Train Nr</th>
                <th data-col="art">Type</th>
                <th data-col="gattung">Category</th>
                <th data-col="begin">Start Time</th>
                <th data-col="fahrzeit">Duration</th>
                <th data-col="br">BR</th>
                <th data-col="laenge">Length</th>
                <th data-col="masse">Mass</th>
                <th data-col="nhalte">Stops</th>
                <th data-col="ev">EV</th>
                <th data-col="w1">W1</th>
                <th data-col="start">Start</th>
                <th data-col="ende">End</th>
                <th data-col="start_halt">Start Halt</th>
                <th data-col="end_halt">End Halt</th>
                <th data-col="s_km">S KM</th>
                <th data-col="dv">DV</th>
                <th data-col="country">Country</th>
                <th data-col="route">Route</th>
                <th data-col="fahrplan">Fahrplan</th>
                <th data-col="aufgleispunkt">Aufgleispunkt</th>
                <th data-col="zuglauf">Zuglauf</th>
                <th data-col="halte">Halte</th>
            </tr>
        </thead>
        <tbody id="results-body"></tbody>
    </table>

    <div id="stops-modal" class="modal">
        <div class="modal-content">
            <span class="close">×</span>
            <h2>Stops</h2>
            <div id="stops-content"></div>
        </div>
    </div>

    <script>
        const trainData = {train_data_json};
    
        const debug = document.getElementById('debug');
        const countryList = document.getElementById('country-list');
        const countrySelectAll = document.getElementById('country-select-all');
        const countryDeselectAll = document.getElementById('country-deselect-all');
        const routeList = document.getElementById('route-list');
        const routeSelectAll = document.getElementById('route-select-all');
        const routeDeselectAll = document.getElementById('route-deselect-all');
        const brList = document.getElementById('br-list');
        const brSelectAll = document.getElementById('br-select-all');
        const brDeselectAll = document.getElementById('br-deselect-all');
        const artFilter = document.getElementById('filter-art');
        const gattungFilter = document.getElementById('filter-gattung');
        const exactGattung = document.getElementById('exact-gattung');
        const zugnrFilter = document.getElementById('filter-zugnr');
        const exactZugnr = document.getElementById('exact-zugnr');
        const beginMinFilter = document.getElementById('filter-begin-min');
        const beginMaxFilter = document.getElementById('filter-begin-max');
        const fahrzeitMinFilter = document.getElementById('filter-fahrzeit-min');
        const fahrzeitMaxFilter = document.getElementById('filter-fahrzeit-max');
        const brFilter = document.getElementById('filter-br');
        const exactBR = document.getElementById('exact-br');
        const skmMinFilter = document.getElementById('filter-skm-min');
        const skmMaxFilter = document.getElementById('filter-skm-max');
        const dvMinFilter = document.getElementById('filter-dv-min');
        const dvMaxFilter = document.getElementById('filter-dv-max');
        const laengeMinFilter = document.getElementById('filter-laenge-min');
        const laengeMaxFilter = document.getElementById('filter-laenge-max');
        const masseMinFilter = document.getElementById('filter-masse-min');
        const masseMaxFilter = document.getElementById('filter-masse-max');
        const startList = document.getElementById('start-list');
        const startSelectAll = document.getElementById('start-select-all');
        const startDeselectAll = document.getElementById('start-deselect-all');
        const startHaltFilter = document.getElementById('filter-start-halt');
        const endList = document.getElementById('end-list');
        const endSelectAll = document.getElementById('end-select-all');
        const endDeselectAll = document.getElementById('end-deselect-all');
        const endHaltFilter = document.getElementById('filter-end-halt');
        const nhalteMinFilter = document.getElementById('filter-nhalte-min');
        const nhalteMaxFilter = document.getElementById('filter-nhalte-max');
        const evFilter = document.getElementById('filter-ev');
        const w1MinFilter = document.getElementById('filter-w1-min');
        const w1MaxFilter = document.getElementById('filter-w1-max');
        const resultsBody = document.getElementById('results-body');
        const resultsCount = document.getElementById('results-count');
        const stopsModal = document.getElementById('stops-modal');
        const stopsContent = document.getElementById('stops-content');
        const closeModal = document.getElementsByClassName('close')[0];
    
        debug.innerHTML = `Loaded ${trainData.length} records. Sample: ${JSON.stringify(trainData[0], null, 2)}`;
    
        // Track manual manipulation of range selectors
        const rangeManipulated = {
            nhalteMin: false,
            nhalteMax: false,
            w1Min: false,
            w1Max: false
        };
    
        // Store last state of each checkbox list
        let lastCountryState = { values: [], checked: new Set(), active: [] };
        let lastRouteState = { values: [], checked: new Set(), active: [] };
        let lastBRState = { values: [], checked: new Set() };
        let lastStartState = { values: [], checked: new Set(), active: [] };
        let lastEndState = { values: [], checked: new Set(), active: [] };
    
        function timeToMinutes(timeStr) {
            if (!timeStr) return null;
            const [hours, minutes] = timeStr.split(':').map(Number);
            return hours * 60 + minutes;
        }
    
        function populateCheckboxList(container, values, className, previouslyChecked, showAllWithDisabled = false, activeValues = null) {
            container.innerHTML = '';
            values.forEach(value => {
                const div = document.createElement('div');
                div.className = 'checkbox-item';
                const isActive = showAllWithDisabled ? activeValues.includes(value) : true;
                const wasChecked = previouslyChecked.has(value) && isActive;
                div.innerHTML = `<label><input type="checkbox" class="${className}" value="${value}" ${wasChecked ? 'checked' : ''} ${showAllWithDisabled && !isActive ? 'disabled' : ''}> ${value}</label>`;
                if (showAllWithDisabled && !isActive) div.classList.add('disabled');
                container.appendChild(div);
            });
        }
    
        function populateDropdown(select, values) {
            const currentValue = select.value ? Number(select.value) : null;
            const isMinSelector = select === nhalteMinFilter || select === w1MinFilter;
            const baselineMin = Math.min(...values);
            const baselineMax = Math.max(...values);
            let selectedValue;
    
            if ((isMinSelector && !rangeManipulated[select.id.replace('filter-', '')]) ||
                (!isMinSelector && !rangeManipulated[select.id.replace('filter-', '')])) {
                selectedValue = isMinSelector ? baselineMin : baselineMax;
            } else {
                selectedValue = currentValue;
            }
    
            select.innerHTML = '';
            values.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });
    
            if (selectedValue !== null && !values.includes(selectedValue)) {
                const option = document.createElement('option');
                option.value = selectedValue;
                option.textContent = selectedValue;
                select.appendChild(option);
            }
    
            select.value = selectedValue !== null ? selectedValue : (isMinSelector ? baselineMin : baselineMax);
        }
    
        function getCountryFilteredData() {
            const selectedCountries = Array.from(document.querySelectorAll('.country-checkbox:checked')).map(cb => cb.value);
            return trainData.filter(item => 
                selectedCountries.length === 0 || (item.country && selectedCountries.includes(item.country))
            );
        }
    
        function getBaselineData(countryFilteredData) {
            const selectedStarts = Array.from(document.querySelectorAll('.start-checkbox:checked')).map(cb => cb.value);
            const selectedEnds = Array.from(document.querySelectorAll('.end-checkbox:checked')).map(cb => cb.value);
            const gattungTerms = gattungFilter.value.trim().split(/\s+/).filter(t => t);
            const nhalteMin = nhalteMinFilter.value !== '' ? Number(nhalteMinFilter.value) : null; // Moved here
            const nhalteMax = nhalteMaxFilter.value !== '' ? Number(nhalteMaxFilter.value) : null; // Moved here
    
            return countryFilteredData.filter(item => {
                const beginMinutes = timeToMinutes(item.begin);
                const beginMin = beginMinFilter.value ? timeToMinutes(beginMinFilter.value) : null;
                const beginMax = beginMaxFilter.value ? timeToMinutes(beginMaxFilter.value) : null;
                const fahrzeitMinutes = timeToMinutes(item.fahrzeit);
                const fahrzeitMin = fahrzeitMinFilter.value ? timeToMinutes(fahrzeitMinFilter.value) : null;
                const fahrzeitMax = fahrzeitMaxFilter.value ? timeToMinutes(fahrzeitMaxFilter.value) : null;
                const skmMin = skmMinFilter.value ? Number(skmMinFilter.value) : null;
                const skmMax = skmMaxFilter.value ? Number(skmMaxFilter.value) : null;
                const dvMin = dvMinFilter.value ? Number(dvMinFilter.value) : null;
                const dvMax = dvMaxFilter.value ? Number(dvMaxFilter.value) : null;
                const laengeMin = laengeMinFilter.value ? Number(laengeMinFilter.value) : null;
                const laengeMax = laengeMaxFilter.value ? Number(laengeMaxFilter.value) : null;
                const masseMin = masseMinFilter.value ? Number(masseMinFilter.value) : null;
                const masseMax = masseMaxFilter.value ? Number(masseMaxFilter.value) : null;
    
                return (
                    (!artFilter.value || (item.art && item.art === artFilter.value)) &&
                    (!gattungTerms.length || (exactGattung.checked ? item.gattung === gattungFilter.value : gattungTerms.every(term => item.gattung && item.gattung.includes(term)))) &&
                    (!zugnrFilter.value || (item.zugnr && (exactZugnr.checked ? item.zugnr.toString() === zugnrFilter.value : item.zugnr.toString().includes(zugnrFilter.value)))) &&
                    (!beginMin || (beginMinutes && beginMinutes >= beginMin)) &&
                    (!beginMax || (beginMinutes && beginMinutes <= beginMax)) &&
                    (!fahrzeitMin || (fahrzeitMinutes && fahrzeitMinutes >= fahrzeitMin)) &&
                    (!fahrzeitMax || (fahrzeitMinutes && fahrzeitMinutes <= fahrzeitMax)) &&
                    (!skmMin || (item.s_km && item.s_km >= skmMin)) &&
                    (!skmMax || (item.s_km && item.s_km <= skmMax)) &&
                    (!dvMin || (item.dv && item.dv >= dvMin)) &&
                    (!dvMax || (item.dv && item.dv <= dvMax)) &&
                    (!laengeMin || (item.laenge && item.laenge >= laengeMin)) &&
                    (!laengeMax || (item.laenge && item.laenge <= laengeMax)) &&
                    (!masseMin || (item.masse && item.masse >= masseMin)) &&
                    (!masseMax || (item.masse && item.masse <= masseMax)) &&
                    (nhalteMin === null || (item.nhalte !== null && item.nhalte >= nhalteMin)) && // Added here
                    (nhalteMax === null || (item.nhalte !== null && item.nhalte <= nhalteMax)) && // Added here
                    (selectedStarts.length === 0 || (item.start && selectedStarts.includes(item.start))) &&
                    (!startHaltFilter.checked || (item.start_halt && item.start_halt === 1)) &&
                    (selectedEnds.length === 0 || (item.ende && selectedEnds.includes(item.ende))) &&
                    (!endHaltFilter.checked || (item.end_halt && item.end_halt === 1)) &&
                    (!evFilter.checked || (item.ev && item.ev > 0))
                );
            });
        }
    
        function filterData() {
            const countryFilteredData = getCountryFilteredData();
            const baselineData = getBaselineData(countryFilteredData);
            const selectedRoutes = Array.from(document.querySelectorAll('.route-checkbox:checked')).map(cb => cb.value);
            const selectedBRs = Array.from(document.querySelectorAll('.br-checkbox:checked')).map(cb => cb.value);
            const brTerms = brFilter.value.trim().split(/\s+/).filter(t => t);
            const w1Min = w1MinFilter.value !== '' ? Number(w1MinFilter.value) : null;
            const w1Max = w1MaxFilter.value !== '' ? Number(w1MaxFilter.value) : null;
    
            let brFilteredData = baselineData.filter(item =>
                (!brFilter.value || (item.br && (exactBR.checked ? item.br === brFilter.value : brTerms.every(term => item.br.includes(term)))))
            );
    
            let routeFilteredData = brFilteredData.filter(item =>
                (selectedRoutes.length === 0 || (item.route && selectedRoutes.includes(item.route)))
            );
    
            const filtered = routeFilteredData.filter(item =>
                (selectedBRs.length === 0 || (item.br && selectedBRs.includes(item.br))) &&
                (w1Min === null || (item.w1 !== null && item.w1 >= w1Min)) &&
                (w1Max === null || (item.w1 !== null && item.w1 <= w1Max))
            );
    
            updateFilters(countryFilteredData, baselineData, brFilteredData, routeFilteredData, filtered);
            displayResults(filtered);
        }
    
        function updateFilters(countryFilteredData, baselineData, brFilteredData, routeFilteredData, filteredData) {
            const countryChecked = new Set(Array.from(document.querySelectorAll('.country-checkbox:checked')).map(cb => cb.value));
            const routeChecked = new Set(Array.from(document.querySelectorAll('.route-checkbox:checked')).map(cb => cb.value));
            const brChecked = new Set(Array.from(document.querySelectorAll('.br-checkbox:checked')).map(cb => cb.value));
            const startChecked = new Set(Array.from(document.querySelectorAll('.start-checkbox:checked')).map(cb => cb.value));
            const endChecked = new Set(Array.from(document.querySelectorAll('.end-checkbox:checked')).map(cb => cb.value));
    
            const allCountries = [...new Set(trainData.map(item => item.country).filter(v => v))].sort();
            const allRoutes = [...new Set(countryFilteredData.map(item => item.route).filter(v => v))].sort();
            const allBRs = [...new Set(routeFilteredData.map(item => item.br).filter(v => v))].sort();
            const allStarts = [...new Set(trainData.map(item => item.start).filter(v => v))].sort();
            const allEnds = [...new Set(trainData.map(item => item.ende).filter(v => v))].sort();
    
            const activeCountries = [...new Set(filteredData.map(item => item.country).filter(v => v))].sort();
            const activeRoutes = [...new Set(brFilteredData.map(item => item.route).filter(v => v))].sort();
            const activeStarts = [...new Set(filteredData.map(item => item.start).filter(v => v))].sort();
            const activeEnds = [...new Set(filteredData.map(item => item.ende).filter(v => v))].sort();
    
            const baselineNhalteValues = [...new Set(routeFilteredData.map(item => item.nhalte).filter(v => v != null))].sort((a, b) => a - b);
            const baselineW1Values = [...new Set(routeFilteredData.map(item => item.w1).filter(v => v != null))].sort((a, b) => a - b);
    
            // Update countries list only if none are checked
            if (countryChecked.size === 0) {
                populateCheckboxList(countryList, allCountries, 'country-checkbox', countryChecked, true, activeCountries);
                lastCountryState = { values: allCountries, checked: new Set(), active: activeCountries };
            } else {
                populateCheckboxList(countryList, lastCountryState.values, 'country-checkbox', countryChecked, true, lastCountryState.active);
            }
    
            // Update routes list only if none are checked
            if (routeChecked.size === 0) {
                populateCheckboxList(routeList, allRoutes, 'route-checkbox', routeChecked, true, activeRoutes);
                lastRouteState = { values: allRoutes, checked: new Set(), active: activeRoutes };
            } else {
                populateCheckboxList(routeList, lastRouteState.values, 'route-checkbox', routeChecked, true, lastRouteState.active);
            }
    
            // Update BR list only if none are checked
            if (brChecked.size === 0) {
                populateCheckboxList(brList, allBRs, 'br-checkbox', brChecked);
                lastBRState = { values: allBRs, checked: new Set() };
            } else {
                populateCheckboxList(brList, lastBRState.values, 'br-checkbox', brChecked);
            }
    
            // Update start types list only if none are checked
            if (startChecked.size === 0) {
                populateCheckboxList(startList, allStarts, 'start-checkbox', startChecked, true, activeStarts);
                lastStartState = { values: allStarts, checked: new Set(), active: activeStarts };
            } else {
                populateCheckboxList(startList, lastStartState.values, 'start-checkbox', startChecked, true, lastStartState.active);
            }
    
            // Update end types list only if none are checked
            if (endChecked.size === 0) {
                populateCheckboxList(endList, allEnds, 'end-checkbox', endChecked, true, activeEnds);
                lastEndState = { values: allEnds, checked: new Set(), active: activeEnds };
            } else {
                populateCheckboxList(endList, lastEndState.values, 'end-checkbox', endChecked, true, lastEndState.active);
            }
    
            if (baselineNhalteValues.length > 0) {
                populateDropdown(nhalteMinFilter, baselineNhalteValues);
                populateDropdown(nhalteMaxFilter, baselineNhalteValues);
            }
            if (baselineW1Values.length > 0) {
                populateDropdown(w1MinFilter, baselineW1Values);
                populateDropdown(w1MaxFilter, baselineW1Values);
            }
    
            document.querySelectorAll('.country-checkbox, .route-checkbox, .br-checkbox, .start-checkbox, .end-checkbox').forEach(cb => {
                cb.addEventListener('change', filterData);
            });
        }
    
        function displayResults(data) {
            const selectedCountries = Array.from(document.querySelectorAll('.country-checkbox:checked')).map(cb => cb.value);
            const selectedRoutes = Array.from(document.querySelectorAll('.route-checkbox:checked')).map(cb => cb.value);
            const selectedBRs = Array.from(document.querySelectorAll('.br-checkbox:checked')).map(cb => cb.value);
            const selectedStarts = Array.from(document.querySelectorAll('.start-checkbox:checked')).map(cb => cb.value);
            const selectedEnds = Array.from(document.querySelectorAll('.end-checkbox:checked')).map(cb => cb.value);
    
            const hiddenCols = new Set();
            if (artFilter.value) hiddenCols.add('art');
            if (evFilter.checked) hiddenCols.add('ev');
            if (w1MinFilter.value !== '' || w1MaxFilter.value !== '') hiddenCols.add('w1');
            if (selectedCountries.length === 1) hiddenCols.add('country');
            if (selectedRoutes.length === 1) hiddenCols.add('route');
            if (selectedBRs.length === 1) hiddenCols.add('br');
            if (selectedStarts.length === 1) hiddenCols.add('start');
            if (selectedEnds.length === 1) hiddenCols.add('ende');
    
            document.querySelectorAll('#results-table th').forEach(th => {
                const col = th.getAttribute('data-col');
                th.style.display = hiddenCols.has(col) ? 'none' : '';
            });
    
            resultsBody.innerHTML = '';
            resultsCount.textContent = `Showing ${data.length} results`;
            data.forEach((item, index) => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td ${hiddenCols.has('zugnr') ? 'style="display:none"' : ''}>${item.zugnr || ''}</td>
                    <td ${hiddenCols.has('art') ? 'style="display:none"' : ''}>${item.art || ''}</td>
                    <td ${hiddenCols.has('gattung') ? 'style="display:none"' : ''}>${item.gattung || ''}</td>
                    <td ${hiddenCols.has('begin') ? 'style="display:none"' : ''}>${item.begin || ''}</td>
                    <td ${hiddenCols.has('fahrzeit') ? 'style="display:none"' : ''}>${item.fahrzeit || ''}</td>
                    <td ${hiddenCols.has('br') ? 'style="display:none"' : ''}>${item.br || ''}</td>
                    <td ${hiddenCols.has('laenge') ? 'style="display:none"' : ''}>${item.laenge || ''}</td>
                    <td ${hiddenCols.has('masse') ? 'style="display:none"' : ''}>${item.masse || ''}</td>
                    <td ${hiddenCols.has('nhalte') ? 'style="display:none"' : ''}>${item.nhalte || ''}</td>
                    <td ${hiddenCols.has('ev') ? 'style="display:none"' : ''}>${item.ev || ''}</td>
                    <td ${hiddenCols.has('w1') ? 'style="display:none"' : ''}>${item.w1 || ''}</td>
                    <td ${hiddenCols.has('start') ? 'style="display:none"' : ''}>${item.start || ''}</td>
                    <td ${hiddenCols.has('ende') ? 'style="display:none"' : ''}>${item.ende || ''}</td>
                    <td ${hiddenCols.has('start_halt') ? 'style="display:none"' : ''}>${item.start_halt || ''}</td>
                    <td ${hiddenCols.has('end_halt') ? 'style="display:none"' : ''}>${item.end_halt || ''}</td>
                    <td ${hiddenCols.has('s_km') ? 'style="display:none"' : ''}>${item.s_km || ''}</td>
                    <td ${hiddenCols.has('dv') ? 'style="display:none"' : ''}>${item.dv || ''}</td>
                    <td ${hiddenCols.has('country') ? 'style="display:none"' : ''}>${item.country || ''}</td>
                    <td ${hiddenCols.has('route') ? 'style="display:none"' : ''}>${item.route || ''}</td>
                    <td ${hiddenCols.has('fahrplan') ? 'style="display:none"' : ''}>${item.fahrplan || ''}</td>
                    <td ${hiddenCols.has('aufgleispunkt') ? 'style="display:none"' : ''}>${item.aufgleispunkt || ''}</td>
                    <td ${hiddenCols.has('zuglauf') ? 'style="display:none"' : ''}>${item.zuglauf || ''}</td>
                    <td ${hiddenCols.has('halte') ? 'style="display:none"' : ''}><button onclick="showStops(${index})">Show Stops</button></td>
                `;
                resultsBody.appendChild(row);
            });
        }
    
        function showStops(index) {
            const item = trainData[index];
            const stops = (item.halte || '').split(', ').filter(stop => stop.trim());
            stopsContent.innerHTML = stops.length > 0 ? stops.map(stop => `<p>${stop}</p>`).join('') : '<p>No stops available</p>';
            stopsModal.style.display = 'block';
        }
    
        closeModal.onclick = () => stopsModal.style.display = 'none';
        window.onclick = (event) => {
            if (event.target == stopsModal) stopsModal.style.display = 'none';
        };
    
        countrySelectAll.addEventListener('click', () => {
            document.querySelectorAll('.country-checkbox:not(:disabled)').forEach(cb => cb.checked = true);
            filterData();
        });
        countryDeselectAll.addEventListener('click', () => {
            document.querySelectorAll('.country-checkbox').forEach(cb => cb.checked = false);
            filterData();
        });
        routeSelectAll.addEventListener('click', () => {
            document.querySelectorAll('.route-checkbox:not(:disabled)').forEach(cb => cb.checked = true);
            filterData();
        });
        routeDeselectAll.addEventListener('click', () => {
            document.querySelectorAll('.route-checkbox').forEach(cb => cb.checked = false);
            filterData();
        });
        brSelectAll.addEventListener('click', () => {
            document.querySelectorAll('.br-checkbox').forEach(cb => cb.checked = true);
            filterData();
        });
        brDeselectAll.addEventListener('click', () => {
            document.querySelectorAll('.br-checkbox').forEach(cb => cb.checked = false);
            filterData();
        });
        startSelectAll.addEventListener('click', () => {
            document.querySelectorAll('.start-checkbox:not(:disabled)').forEach(cb => cb.checked = true);
            filterData();
        });
        startDeselectAll.addEventListener('click', () => {
            document.querySelectorAll('.start-checkbox').forEach(cb => cb.checked = false);
            filterData();
        });
        endSelectAll.addEventListener('click', () => {
            document.querySelectorAll('.end-checkbox:not(:disabled)').forEach(cb => cb.checked = true);
            filterData();
        });
        endDeselectAll.addEventListener('click', () => {
            document.querySelectorAll('.end-checkbox').forEach(cb => cb.checked = false);
            filterData();
        });
    
        [nhalteMinFilter, nhalteMaxFilter, w1MinFilter, w1MaxFilter].forEach(element => {
            element.addEventListener('change', (e) => {
                rangeManipulated[e.target.id.replace('filter-', '')] = true;
                filterData();
            });
        });
    
        [artFilter, gattungFilter, exactGattung, zugnrFilter, exactZugnr, beginMinFilter, beginMaxFilter, 
         fahrzeitMinFilter, fahrzeitMaxFilter, brFilter, exactBR, skmMinFilter, skmMaxFilter, dvMinFilter, dvMaxFilter,
         laengeMinFilter, laengeMaxFilter, masseMinFilter, masseMaxFilter,
         startHaltFilter, endHaltFilter, evFilter].forEach(element => {
            element.addEventListener('change', filterData);
            element.addEventListener('keyup', filterData);
        });
    
        filterData();
    </script>
</body>
</html>
"""

def fetch_train_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM _00_latest")
    rows = cursor.fetchall()
    train_data = [dict(row) for row in rows]
    conn.close()
    return train_data

def generate_html():
    train_data = fetch_train_data()
    train_data_json = json.dumps(train_data, ensure_ascii=False)
    html_content = HTML_TEMPLATE.replace('{train_data_json}', train_data_json)

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"Generated HTML file at: {OUTPUT_HTML}")
    print(f"Total train services processed: {len(train_data)}")

if __name__ == "__main__":
    os.makedirs(WEB_DIR, exist_ok=True)
    generate_html()
