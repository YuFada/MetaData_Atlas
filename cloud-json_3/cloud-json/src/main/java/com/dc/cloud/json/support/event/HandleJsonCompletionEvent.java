package com.dc.cloud.json.support.event;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.ApplicationEvent;

import java.util.List;

@Getter
@Setter
public class HandleJsonCompletionEvent extends ApplicationEvent {

    private String message;

    private HandleJsonEnum handleJsonEnum;

    public HandleJsonCompletionEvent(List hiveData, String message,HandleJsonEnum handleJsonEnum) {
        super(hiveData);
        this.message = message;
        this.handleJsonEnum = handleJsonEnum;
    }


    @SuppressWarnings("unchecked")
    public <T> List<T> getHiveData(){
        return (List<T>) getSource();
    }






}
