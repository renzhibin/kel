package org.csits.kel.web;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = GitPropertiesController.class)
class GitPropertiesControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void health_returns200AndStatusUp() throws Exception {
        mockMvc.perform(get("/api/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void git_returns200AndBranchCommitId() throws Exception {
        mockMvc.perform(get("/api/git"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.branch").exists())
            .andExpect(jsonPath("$.commitId").exists())
            .andExpect(jsonPath("$.branch").value("unknown"))
            .andExpect(jsonPath("$.commitId").value("unknown"));
    }
}
