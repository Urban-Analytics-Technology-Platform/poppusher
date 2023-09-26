<script lang="ts">
	import { metricTree, selectedMetrics } from '../../stores/metrics';
	import * as _ from 'lodash';
	import TreeView from '../TreeView/TreeView.svelte';

	let search: string | null = null;
	let selectedUniverse: string | null = null;
	let selectedTree: any | null = null;

	$: {
		if ($metricTree.loading === false && selectedUniverse) {
			selectedTree = $metricTree.value.find((node) => node.name === selectedUniverse);
			console.log('Tree ', selectedTree);
		}
	}

	function updateSearch(e) {
		let val = e.target.value;
		search = val;
	}
</script>

{#if $metricTree.loading}
	<p>Loading...</p>
{:else}
	<div class="container">
		<div class="column">
			<sp-search value={search ?? ''} on:input={updateSearch} />
			<sp-menu>
				{#each $metricTree.value
					.map((v) => v.name)
					.filter((v) => (search ? v
									.toLowerCase()
									.includes(search.toLowerCase()) : true)) as universe}
					<sp-menu-item
						class={universe === selectedUniverse ? 'selected' : ''}
						on:click={() => (selectedUniverse = universe)}
					>
						{universe}
					</sp-menu-item>
				{/each}
			</sp-menu>
		</div>
		<div class="column">
			{#if selectedTree}
				<TreeView tree={selectedTree} />
			{/if}
		</div>
	</div>
{/if}

<style>
	.column {
		display: flex;
		flex-direction: column;
		max-width: 300px;
		overflow-y: auto;
	}
	.picker {
		max-height: 500px;
	}
	.topics {
		max-height: 500px;
		overflow-y: auto;
	}
	.container {
		display: flex;
		flex-direction: row;
		max-height: 400px;
	}
	.selected {
		font-weight: bold;
	}
</style>
