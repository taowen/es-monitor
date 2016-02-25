<template>
  <div class="grid">
    <div class="col grid" push-left="off-1" push-right="off-1" id="epl-q">
      <div class="col-12">
        <label for="epl-q-sql">SQL:</label>
      </div>
      <div class="col-12">
        <textarea id="epl-q-sql" v-model="sql"></textarea>
      </div>
      <div class="col-12">
        <button id="epl-q-query" @click="query()">Query</button>
      </div>
      <div class="col-12">
        <textarea v-model="response" style="width: 100%; height: 10em;"></textarea>
      </div>
    </div>
  </div>
</template>

<script type="text/ecmascript-6">
  export default {
    data () {
      return {
        sql: 'SELECT COUNT(*) FROM quote;'
      }
    },
    methods: {
      query () {
        this.$http.post('/translate', this.sql).then(
            response => {
              this.$set('response', JSON.stringify(response.data));
            },
            response => {
            }
        )
      }
    }
  }
</script>

<style scoped>
  #epl-q #epl-q-sql {
    width: 100%;
    height: 10em;
  }

  #epl-q #epl-q-query {
    width: 5em;
    height: 2em;
  }
</style>