<script lang="ts">
	import VirtualList from '@sveltejs/svelte-virtual-list';
	export let selectedPath: Array<string> = [];
	export let options: Array<{ path: Array<string>; name: string; id: string }>;

	function optionsForLevel(level: number) {
		const levelOptions = options.filter((option) => option.path.length == level + 1);
		console.log(level, 'Level options ', levelOptions);
		const result = levelOptions.filter((o) => {
			return (
				JSON.stringify(o.path.slice(0, level)) === JSON.stringify(selectedPath.slice(0, level))
			);
		});
		console.log(level, 'result ', result);
		return result;
	}

	console.log(' selected Path  ', selectedPath);
</script>

<div class="container">
	<div class="column">
		<sp-menu>
			{#each optionsForLevel(0) as option}
				<sp-menu-item on:click={() => (selectedPath = option.path)}>
					<slot item={option}>option.name</slot>
				</sp-menu-item>
			{/each}
		</sp-menu>
	</div>

	{#each selectedPath as pathSegment, level}
		<div class="column">
			<sp-menu>
				<VirtualList items={optionsForLevel(level + 1)} let:item>
					<sp-menu-item on:click|self={() => (selectedPath = item.path)}>
						<slot {item}>{item.name}</slot>
					</sp-menu-item>
				</VirtualList>
			</sp-menu>
		</div>
	{/each}
</div>

<style>
	.container {
		display: flex;
		flex-direction: row;
		overflow-x: auto;
		max-height: 400px;
	}
	.column {
		max-width: 300px;
		border-right: 1px solid grey;
	}
</style>
