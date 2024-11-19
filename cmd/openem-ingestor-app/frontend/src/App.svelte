<script lang="ts">
  import logo from "./assets/images/logo-wide-1024x317.png";
  import {
    ExtractMetadata,
    SelectFolder,
    CancelTask,
    RemoveTask,
    ScheduleTask,
    AvailableMethods,
  } from "../wailsjs/go/main/App.js";
  import { EventsOn } from "../wailsjs/runtime/runtime";
  import List from "./List.svelte";
  import ListElement from "./ListElement.svelte";

  let selected_extractor;

  async function extractMetadata(id: string): Promise<string> {
    return await ExtractMetadata(selected_extractor, id);
  }
  function selectFolder(): void {
    SelectFolder();
  }

  function cancelTask(id): void {
    CancelTask(id);
  }
  function removeTask(id: string): void {
    RemoveTask(id);
  }

  function secondsToStr(elapsed_seconds): string {
    return new Date(elapsed_seconds * 1000).toISOString().substr(11, 8);
  }

  function scheduleTask(id: string): void {
    ScheduleTask(id);
  }

  let items = {};

  function newItem(id: string, folder: string): string {
    items[id] = {
      id: id,
      value: folder,
      status: "Selected",
      progress: 0,
      component: ListElement,
      extractMetadata: extractMetadata,
      cancelTask: cancelTask,
      scheduleTask: scheduleTask,
      removeTask: removeTask,
    };
    return id;
  }

  let extractors = ["No extractors found"];
  let schemas = {};

  async function refreshExtractors() {
    AvailableMethods().then((a) => {
      extractors = [];
      a.forEach((element) => {
        extractors.push(element.Name);
        schemas[element.Name] = atob(element.Schema);
      });
      if (extractors.length > 0) selected_extractor = extractors[0];
      else selected_extractor = ["No extractors found"];
    });
  }

  window.onload = refreshExtractors;

  EventsOn("folder-added", (id, folder) => {
    newItem(id, folder);
  });

  EventsOn("folder-removed", (id) => {
    delete items[id];
    items = items;
  });

  EventsOn("upload-scheduled", (id) => {
    items[id].status = "Scheduled";
    items = items;
  });

  EventsOn("upload-completed", (id, elapsed_seconds) => {
    items[id].status = "Completed in " + secondsToStr(elapsed_seconds);
    items = items;
  });

  EventsOn("upload-failed", (id, err) => {
    items[id].status = "failed " + err;
    items = items;
  });

  EventsOn("upload-canceled", (id) => {
    console.log(id);
    items[id].status = "Canceled";
    items = items;
  });

  EventsOn("log-update", (id, message) => {
    console.log(id);
    items[id].status += "\n" + message;
    items = items;
  });

  EventsOn("progress-update", (id, percentage, elapsed_seconds) => {
    items[id].progress = percentage.toFixed(0);
    items[id].status += "\n" + "Uploading... " + secondsToStr(elapsed_seconds);
  });
</script>

<main>
  <img alt="OpenEM logo" id="logo" src={logo} height="200px" />

  <div>
    <h3>Metadata Extractors</h3>
    <select bind:value={selected_extractor}>
      {#each extractors as extractor}
        <option value={extractor}>
          {extractor}
        </option>
      {/each}
    </select>
    <button class="btn" on:click={refreshExtractors}> Refresh </button>
  </div>
  <div>
    <textarea
      style="height:200px; width:400px"
      bind:value={schemas[selected_extractor]}
    />
  </div>

  <h3>Datasets</h3>
  <button class="btn" on:click={selectFolder}>Select Folder</button>
  <div>
    <div id="upload-list">
      <List {items} />
    </div>
  </div>
</main>

<style>
  #logo {
    display: block;
    width: 20%;
    height: 20%;
    margin: auto;
    padding: 10% 0 0;
    background-position: center;
    background-repeat: no-repeat;
    background-size: 100% 100%;
    background-origin: content-box;
  }
</style>
