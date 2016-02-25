<template>
  <div id="epl-main">
    <query></query>
  </div>
</template>

<script>
  import Query from './components/Query'
  var Vue = require('vue')

  Vue.use(require('vue-resource'))
  export default {
    components: {
      Query
    }
  }
</script>

<style scoped>
  #epl-main {
    margin-top: 1em;
    margin-right: 1em;
  }
</style>
