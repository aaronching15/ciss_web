<!-- 脚本 -->
<template>
  <div id="app">
    <img src="./assets/logo.png">
    <router-view/>
    <h1>{{ msg }} </h1>
    <compo_ciss></compo_ciss>

  </div>
</template>
<!-- 脚本 -->

<script>
import compo_ciss from './component/compo_ciss.vue'
export default {
  data () {
    return {
      msg : 'Menu'
    }
  },
  components : { compo_ciss }
}
</script>

<!-- 样式代码 -->
<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
