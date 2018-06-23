package com.imooc.dao;

import com.imooc.domain.CourseClickCount;
import com.imooc.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 实战课程访问数量数据访问层
 */
@Component
public class CourseClickCountDAO
{


    /**
     * 根据天查询
     */
    public List<CourseClickCount> query(String day) throws Exception
    {

        List<CourseClickCount> list = new ArrayList<>();

        // 去HBase表中根据day获取实战课程对应的访问量
        Map<String, Long> map = HBaseUtils.getInstance().query("boy_imooc_course_clickcount", "20180622");

        for (Map.Entry<String, Long> entry : map.entrySet())
        {
            CourseClickCount model = new CourseClickCount();
            model.setName(entry.getKey());
            model.setValue(entry.getValue());

            list.add(model);
        }

        return list;
    }

    public static void main(String[] args) throws Exception
    {
        CourseClickCountDAO dao = new CourseClickCountDAO();
        List<CourseClickCount> list = dao.query("20180622");
        for (CourseClickCount model : list)
        {
            System.out.println(model.getName() + " : " + model.getValue());
        }

        /**结果：
         * 20180622_128 : 351
         * 20180622_130 : 366
         * 20180622_131 : 332
         * 20180622_112 : 354
         * 20180622_145 : 348
         * 20180622_146 : 348
         */
    }

}
