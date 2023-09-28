<script lang="ts">
	import { MapLibre, FillLayer, LineLayer, Popup } from 'svelte-maplibre';
	import VectorTileSource from '../VectorTileSource/VectorTileSource.svelte';
	import { metricValues, selectedMetric } from '../../stores/metrics';
	import JoinedData from '../JoinedData/JoinedData.svelte';
	import { quantile } from 'd3';

	let colors = ['#d3f2a3', '#97e196', '#6cc08b', '#4c9b82', '#217a79', '#105965', '#074050'];

	let dataStyle: Array<any>;
	$: {
		console.log('Computing data style ');
		if ($metricValues.value) {
			let vals = $metricValues.value.map((v) => v[$selectedMetric]);
			let quantiles = colors.map((_, index) => quantile(vals, (index + 1) / colors.length));
			let colVals: Array<any> = [];
			for (const index in quantiles) colVals.push(quantiles[index]!, colors[index]);

			// dataStyle = ['rgb', ['number', ['feature-state', selectedMetric]], 0, 0];

			dataStyle = [
				'interpolate',
				['linear'],
				['number', ['feature-state', $selectedMetric]],
				...colVals
			];
		}
		console.log('Computed data style ', dataStyle);
	}
</script>

<div class="mapcontainer">
	<MapLibre
		style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
		class={'map'}
		standardControls
		center={[-87.622088, 41.878781]}
		zoom={10}
	>
		<VectorTileSource
			url="pmtiles://https://allofthedata.s3.us-west-2.amazonaws.com/acs/tracts_2019.pmtiles"
			promoteId="GEOID"
		>
			<FillLayer
				paint={{
					'fill-opacity': 0.6,
					'fill-color': dataStyle ?? ['rgb', 255, 0, 0]
				}}
				sourceLayer={'tractsgeojsonseq'}
			>
				<Popup openOn="hover" let:features>
					<div class="flex flex-col gap-2">
						<p>Population: {JSON.stringify(features && features[0].state, null, 2)}</p>
					</div>
				</Popup>
			</FillLayer>
			<LineLayer
				paint={{
					'line-opacity': 0.6,
					'line-color': 'rgb(200, 200, 200)',
					'line-width': 1
				}}
				sourceLayer={'tractsgeojsonseq'}
			/>
			{#if $metricValues.loading === false && $metricValues.value}
				<JoinedData
					data={$metricValues.value.map((v) => ({ ...v, GEO_ID: v.GEO_ID.split('US')[1] }))}
					idCol="GEO_ID"
					sourceLayer="tractsgeojsonseq"
				/>
			{/if}
		</VectorTileSource>
	</MapLibre>
</div>

<style>
	:global(.map) {
		width: 100%;
		height: 500px;
	}
</style>
